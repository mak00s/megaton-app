from copy import deepcopy
import json
from unittest.mock import Mock

import pytest

from megaton_lib.docs_client import DocsClient, DocsEditError
from megaton_lib import docs_mutations as m
from megaton_lib import docs_mutate as cli


def paragraph(text, *, style="NORMAL_TEXT", bullet=False, bold=False):
    return {"paragraph": {"paragraphStyle": {"namedStyleType": style},
                           **({"bullet": {"listId": "list1"}} if bullet else {}),
                           "elements": [{"textRun": {"content": text + "\n", "textStyle": {"bold": bold}}}]}}


def table(values, *, styled=False):
    rows = []
    for r, values_row in enumerate(values):
        rows.append({"tableCells": [
            {"content": [paragraph(v, bold=styled and r in {0, len(values) - 1})],
             "tableCellStyle": {"backgroundColor": m._color("#e8eaed")} if styled and r in {0, len(values) - 1} else {}}
            for v in values_row]})
    return {"table": {"rows": len(values), "columns": len(values[0]), "tableRows": rows, "tableStyle": {}}}


def document(blocks=None, *, revision="r1", tab="t.0", images=None):
    blocks = deepcopy(blocks or [paragraph("Title", style="HEADING_1"), paragraph("Analysis"), paragraph("")])

    def index(block, start):
        block["startIndex"] = start
        if "paragraph" in block:
            for element in block["paragraph"]["elements"]:
                element["startIndex"] = start
                start += m.utf16_length(element["textRun"]["content"]) if "textRun" in element else 1
                element["endIndex"] = start
        else:
            start += 1
            for row in block["table"]["tableRows"]:
                row["startIndex"] = start
                start += 1
                for cell in row["tableCells"]:
                    cell["startIndex"] = start
                    start += 1
                    for p in cell["content"]:
                        start = index(p, start)
                    cell["endIndex"] = start
                row["endIndex"] = start
            start += 1
        block["endIndex"] = start
        return start
    cursor = 1
    for block in blocks:
        cursor = index(block, cursor)
    return {"documentId": "doc1", "revisionId": revision, "tabs": [{
        "tabProperties": {"tabId": tab}, "documentTab": {"body": {"content": blocks}, "inlineObjects": images or {}}}]}


def client_with(*snapshots, revisions=()):
    service = Mock()
    api = service.documents.return_value
    api.get.return_value.execute.side_effect = snapshots
    api.batchUpdate.return_value.execute.side_effect = [
        {"writeControl": {"requiredRevisionId": r}, "replies": []} for r in revisions]
    return DocsClient(service), api


def plan_for(ops, doc=None):
    return m.plan_mutations(doc or document(), operations=ops)


def insert_op():
    return m.insert_paragraphs_after_anchor(m.paragraph_anchor("Title"),
        [{"text": "Comment", "paragraph_style": {"namedStyleType": "HEADING_2"}}, {"text": "😀 finding", "bullet": True}], operation_id="comments")


def apply(client, plan, tmp_path, **kwargs):
    return client.apply_mutation_plan(plan, apply=True, approved_digest=plan.digest,
                                     receipt_path=tmp_path / "receipt.json", **kwargs)


@pytest.mark.parametrize("kind", ["insert_paragraphs", "insert_image", "insert_table", "move_paragraphs"])
@pytest.mark.parametrize("position", ["before", "after"])
def test_table_start_rejected_during_plan(kind, position):
    d = document([paragraph("Source"), paragraph("Before table"), table([["x"]]), paragraph("")])
    anchor = ({"kind": "table", "fingerprint": m.get_normalized_outline(d)[2]["fingerprint"]}
              if position == "before" else m.paragraph_anchor("Before table"))
    op = {"id": "bad", "operation": kind, "anchor": anchor, "position": position}
    if kind == "move_paragraphs":
        op.update(anchor=m.paragraph_anchor("Source"), destination=anchor)
    client, api = client_with(d)
    with pytest.raises(DocsEditError, match='table start.*position="before"'):
        client.plan_mutations("doc1", operations=[op])
    api.batchUpdate.assert_not_called()


@pytest.mark.parametrize("kind", ["insert_paragraphs", "replace_paragraphs"])
def test_new_text_resets_full_mask_including_empty_paragraph(kind):
    op = {"id": "new", "operation": kind, "anchor": m.paragraph_anchor("Title"),
          "paragraphs": [{"text": "new", "text_style": {"bold": True}}, ""]}
    p = plan_for([op]).to_dict()
    requests = m._compile(p["stages"][0], document(), "t.0", "write")
    styles = [r["updateTextStyle"] for r in requests if "updateTextStyle" in r]
    assert [r["textStyle"] for r in styles] == [{"bold": True}, {}]
    assert all(set(r["fields"].split(",")) == m.TEXT_FIELDS for r in styles)
    assert styles[1]["range"]["endIndex"] - styles[1]["range"]["startIndex"] == 1


@pytest.mark.parametrize("extra", [
    {"link": {"url": "https://example.com"}}, {"fontSize": {"magnitude": 18, "unit": "PT"}},
    {"foregroundColor": m._color("#ff0000")}, {"backgroundColor": m._color("#ff0000")},
    {"weightedFontFamily": {"fontFamily": "Arial"}}, {"smallCaps": True}, {"baselineOffset": "SUPERSCRIPT"},
])
def test_reset_verification_rejects_inherited_fields(extra):
    block = paragraph("new")
    block["paragraph"]["elements"][0]["textRun"]["textStyle"].update(extra)
    if "link" in extra:
        block["paragraph"]["elements"][0]["textRun"]["content"] = "new"
        block["paragraph"]["elements"].append({"textRun": {"content": "\n", "textStyle": {}}})
    actual = m.get_normalized_outline(document([block, paragraph("")]))[0]
    expected = m._paragraph("new")
    assert not m._node_matches(expected, actual, {})
    expected["text_style"].update(extra)
    assert m._node_matches(expected, actual, {})
    expected["text_style"] = {}
    expected["text_style_mode"] = "patch"
    assert m._node_matches(expected, actual, {})


def test_reset_verifies_newline_and_accepts_default_false():
    expected = m._paragraph("")
    block = paragraph("")
    actual = m.get_normalized_outline(document([block]))[0]
    assert m._node_matches(expected, actual, {})
    block["paragraph"]["elements"][0]["textRun"]["textStyle"]["link"] = {"url": "https://example.com"}
    actual = m.get_normalized_outline(document([block]))[0]
    assert not m._node_matches(expected, actual, {})


def test_api_normalized_color_font_and_newline_link():
    expected = m._paragraph({'text': 'Linked', 'text_style': {
        'weightedFontFamily': {'fontFamily': 'Arial'}, 'link': {'url': 'https://example.com'},
        'foregroundColor': {'color': {'rgbColor': {'red': 0.1}}}}})
    actual = {'kind': 'paragraph', 'text': 'Linked\n', 'bullet': False,
              'paragraph_style': {'namedStyleType': 'NORMAL_TEXT'},
              'inherited_text_style': {'weightedFontFamily': {'fontFamily': 'Arial', 'weight': 400}},
              'runs': [{'text': 'Linked', 'style': {'link': {'url': 'https://example.com'},
                        'foregroundColor': {'color': {'rgbColor': {'red': 26 / 255}}}}},
                       {'text': '\n', 'style': {'foregroundColor': {'color': {'rgbColor': {'red': 26 / 255}}}}}]}
    assert m._node_matches(expected, actual, {})
    actual['inherited_text_style']['weightedFontFamily']['fontFamily'] = 'Courier New'
    assert not m._node_matches(expected, actual, {})
    actual['inherited_text_style']['weightedFontFamily']['fontFamily'] = 'Arial'
    actual['runs'][0]['style']['foregroundColor']['color']['rgbColor']['red'] = 27 / 255
    assert not m._node_matches(expected, actual, {})


def test_named_style_defaults_are_read_from_selected_tab():
    d = document()
    d['tabs'][0]['documentTab']['namedStyles'] = {'styles': [
        {'namedStyleType': 'NORMAL_TEXT', 'textStyle': {'weightedFontFamily': {'fontFamily': 'Arial'}}}]}
    assert m.get_normalized_outline(d)[0]['inherited_text_style']['weightedFontFamily']['fontFamily'] == 'Arial'


def test_existing_style_patch_keeps_link_and_uses_only_requested_mask():
    block = paragraph("Title")
    block["paragraph"]["elements"][0]["textRun"]["textStyle"]["link"] = {"url": "https://example.com"}
    d = document([block, paragraph("")])
    op = {"id": "bold", "operation": "style_paragraph", "anchor": m.paragraph_anchor("Title"),
          "text_style": {"bold": True}}
    p = plan_for([op], d).to_dict()
    requests = m._compile(p["stages"][0], d, "t.0", "write")
    update = next(r["updateTextStyle"] for r in requests if "updateTextStyle" in r)
    assert update["fields"] == "bold"
    block["paragraph"]["elements"][0]["textRun"]["textStyle"]["bold"] = True
    actual = m.get_normalized_outline(document([block, paragraph("")]))[0]
    assert m._node_matches(p["after"][0], actual, {})


def test_inherited_style_readback_does_not_report_verified(tmp_path):
    plan = plan_for([insert_op()])
    block = paragraph("Comment", style="HEADING_2")
    block["paragraph"]["elements"][0]["textRun"]["textStyle"]["link"] = {"url": "https://example.com"}
    after = document([paragraph("Title", style="HEADING_1"), block, paragraph("😀 finding", bullet=True),
                      paragraph("Analysis"), paragraph("")], revision="r2")
    client, api = client_with(document(), document(), after, revisions=["r2"])
    result = apply(client, plan, tmp_path)
    assert not result["verified"]
    assert not result["ok"]
    assert api.batchUpdate.call_count == 1


def test_move_resets_destination_and_keeps_source_style():
    block = paragraph("Source")
    block["paragraph"]["paragraphStyle"]["direction"] = "LEFT_TO_RIGHT"
    style = {"fontSize": {"magnitude": 18, "unit": "PT"}, "link": {"url": "https://example.com"}}
    block["paragraph"]["elements"][0]["textRun"]["textStyle"] = style
    d = document([block, paragraph("Target"), paragraph("")])
    op = m.move_paragraph_block(m.paragraph_anchor("Source"), m.paragraph_anchor("Source"),
                                m.paragraph_anchor("Target"), operation_id="move")
    p = plan_for([op], d).to_dict()
    requests = m._compile(p["stages"][0], d, "t.0", "write")
    update = next(r["updateTextStyle"] for r in requests if "updateTextStyle" in r)
    assert update["textStyle"] == style
    assert set(update["fields"].split(",")) == m.TEXT_FIELDS
    assert p["after"][1]["text_style_mode"] == "reset"
    paragraph_update = next(r["updateParagraphStyle"] for r in requests if "updateParagraphStyle" in r)
    assert paragraph_update["paragraphStyle"]["direction"] == "LEFT_TO_RIGHT"


@pytest.mark.parametrize("error", [DocsEditError("Anchor must be unique."), RuntimeError("SECRET BODY")])
def test_cli_exposes_only_safe_validation_messages(tmp_path, monkeypatch, capsys, error):
    monkeypatch.setattr(DocsClient, "from_oauth_file", Mock(side_effect=error))
    assert cli.main(["--token", "unused", "--expected-email", "test@example.com", "get", "doc1",
                     "--output", str(tmp_path / "snapshot.json")]) == 1
    output = capsys.readouterr().out
    result = json.loads(output)
    if isinstance(error, DocsEditError):
        assert result["message"] == str(error)
    else:
        assert "message" not in result
        assert "SECRET BODY" not in output


def test_plan_is_deterministic_and_preview_writes_nothing(tmp_path):
    plan = plan_for([insert_op()])
    assert m.DocsMutationPlan.from_dict(plan.to_dict()) == plan
    client, api = client_with()
    assert client.apply_mutation_plan(plan)["write_status"] == "not_started"
    api.get.assert_not_called()
    api.batchUpdate.assert_not_called()
    assert not list(tmp_path.iterdir())
    with pytest.raises(DocsEditError):
        client.apply_mutation_plan(plan, apply=True, approved_digest="bad", receipt_path=tmp_path / "r")


@pytest.mark.parametrize("count", [0, 2])
def test_anchor_must_be_unique(count):
    d = document([*[paragraph("Title") for _ in range(count)], paragraph("")])
    with pytest.raises(DocsEditError, match="anchor match"):
        plan_for([insert_op()], d)


def test_nested_tabs_require_id_and_requests_use_selected_tab():
    d = document(tab="t.outer")
    child = document(tab="t.child")["tabs"][0]
    d["tabs"][0]["childTabs"] = [child]
    with pytest.raises(DocsEditError, match="tab_id"):
        plan_for([insert_op()], d)
    p = m.plan_mutations(d, operations=[insert_op()], tab_id="t.child").to_dict()
    reqs = m._compile(p["stages"][0], d, "t.child", "write")
    for req in reqs:
        value = next(iter(req.values()))
        assert value.get("range", value.get("location"))["tabId"] == "t.child"


def test_apply_paragraphs_bullets_and_utf16(tmp_path):
    plan = plan_for([insert_op()])
    after = document([paragraph("Title", style="HEADING_1"), paragraph("Comment", style="HEADING_2"),
                      paragraph("😀 finding", bullet=True), paragraph("Analysis"), paragraph("")], revision="r2")
    client, api = client_with(document(), document(), after, revisions=["r2"])
    result = apply(client, plan, tmp_path)
    assert result["ok"] and result["verified"]
    assert result["operations"] == [{"operation_id": "comments", "verified": True}]
    requests = api.batchUpdate.call_args.kwargs["body"]["requests"]
    assert requests[0]["insertText"]["text"] == "Comment\n😀 finding\n"
    bullet = next(r["createParagraphBullets"] for r in requests if "createParagraphBullets" in r)
    assert bullet["range"]["endIndex"] - bullet["range"]["startIndex"] == 11
    assert api.batchUpdate.call_args.kwargs["body"]["writeControl"] == {"requiredRevisionId": "r1"}
    api.batchUpdate.return_value.execute.assert_called_once_with(num_retries=0)
    assert (tmp_path / "receipt.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("change", ["revision", "plan", "image"])
def test_stale_inputs_rejected_before_writes(tmp_path, change):
    d = document()
    plan = plan_for([insert_op()])
    if change == "revision":
        d["revisionId"] = "changed"
    elif change == "plan":
        value = plan.to_dict()
        value["stages"][0]["at"] = 0
        with pytest.raises(DocsEditError, match="digest"):
            m.DocsMutationPlan.from_dict(value)
        return
    else:
        from PIL import Image
        path = tmp_path / "chart.png"
        Image.new("RGB", (10, 10)).save(path)
        plan = plan_for([m.insert_image_after_anchor(m.paragraph_anchor("Title"), path,
                                                     width_pt=10, height_pt=10, operation_id="chart")])
        Image.new("RGB", (20, 20)).save(path)
    client, api = client_with(d)
    with pytest.raises(DocsEditError, match="changed"):
        apply(client, plan, tmp_path)
    api.batchUpdate.assert_not_called()


def test_unknown_write_never_retries_or_leaks(tmp_path):
    plan = plan_for([insert_op()])
    client, api = client_with(document(), document())
    api.batchUpdate.return_value.execute.side_effect = TimeoutError("secret token and private document")
    result = apply(client, plan, tmp_path)
    assert not result["ok"] and result["write_status"] == "unknown"
    api.batchUpdate.return_value.execute.assert_called_once_with(num_retries=0)
    assert "secret" not in json.dumps(result)
    with pytest.raises(FileExistsError):
        client2, _ = client_with(document())
        apply(client2, plan, tmp_path)


@pytest.mark.parametrize("mutate", ["text", "style", "bullet", "outside", "revision"])
def test_readback_mismatch_stops(tmp_path, mutate):
    plan = plan_for([insert_op()])
    after = document([paragraph("Title", style="HEADING_1"), paragraph("Comment", style="HEADING_2"),
                      paragraph("😀 finding", bullet=True), paragraph("Analysis"), paragraph("")], revision="r2")
    blocks = after["tabs"][0]["documentTab"]["body"]["content"]
    if mutate == "style":
        blocks[1]["paragraph"]["paragraphStyle"]["namedStyleType"] = "NORMAL_TEXT"
    elif mutate == "bullet":
        del blocks[2]["paragraph"]["bullet"]
    elif mutate == "revision":
        after["revisionId"] = "collaborator"
    else:
        blocks[3 if mutate == "outside" else 1]["paragraph"]["elements"][0]["textRun"]["content"] = "changed\n"
    client, api = client_with(document(), document(), after, revisions=["r2"])
    result = apply(client, plan, tmp_path)
    assert not result["ok"] and not result["verified"]
    assert result["write_status"] == "partial"
    assert api.batchUpdate.call_count == 1


def test_table_6x6_reindexes_values_and_styles(tmp_path):
    values = [[f"{r}😀{c}" for c in range(6)] for r in range(6)]
    plan = plan_for([m.insert_table_after_anchor(m.paragraph_anchor("Title"), values, operation_id="metrics",
                                               header_rows=[0], total_rows=[5])])
    snapshots = []
    for rev, vals, styled in [("r2", [[""] * 6 for _ in range(6)], False), ("r3", values, False), ("r4", values, True)]:
        snapshots.append(document([paragraph("Title", style="HEADING_1"), paragraph(""), table(vals, styled=styled),
                                   paragraph("Analysis"), paragraph("")], revision=rev))
    client, api = client_with(document(), document(), *snapshots, revisions=["r2", "r3", "r4"])
    result = apply(client, plan, tmp_path)
    assert result["ok"], result
    calls = api.batchUpdate.call_args_list
    values_requests = calls[1].kwargs["body"]["requests"]
    indices = [r["insertText"]["location"]["index"] for r in values_requests]
    assert len(indices) == 36 and indices == sorted(indices, reverse=True)
    assert all(c.kwargs["body"]["writeControl"] == {"requiredRevisionId": r} for c, r in zip(calls, ["r1", "r2", "r3"]))
    styles = calls[2].kwargs["body"]["requests"]
    assert len([r for r in styles if "updateTableCellStyle" in r]) == 12
    text_styles = [r["updateTextStyle"] for r in styles if "updateTextStyle" in r]
    assert len(text_styles) >= 36
    assert all(set(r["fields"].split(",")) == m.TEXT_FIELDS for r in text_styles)


def test_remove_bullets_and_style():
    d = document([paragraph("Title", bullet=True), paragraph("")])
    op = {"id": "style", "operation": "style_paragraph", "anchor": m.paragraph_anchor("Title"),
          "bullet": False, "text_style": {"bold": True}}
    p = plan_for([op], d).to_dict()
    reqs = m._compile(p["stages"][0], d, "t.0", "write")
    assert any("deleteParagraphBullets" in r for r in reqs)
    assert not any("createParagraphBullets" in r for r in reqs)


def test_move_keeps_analysis_and_uses_utf16_indices():
    d = document([paragraph("Heading"), paragraph("😀 Analysis"), paragraph("Destination"), paragraph("")])
    op = m.move_paragraph_block(m.paragraph_anchor("😀 Analysis"), m.paragraph_anchor("😀 Analysis"),
                                m.paragraph_anchor("Destination"), operation_id="move")
    p = plan_for([op], d).to_dict()
    assert [n["text"] for n in p["after"]] == ["Heading\n", "Destination\n", "😀 Analysis\n", "\n"]
    reqs = m._compile(p["stages"][0], d, "t.0", "write")
    assert reqs[1]["insertText"]["location"]["index"] == 21


def test_inline_image_id_position_and_size_verified(tmp_path):
    from PIL import Image
    path = tmp_path / "chart.png"
    Image.new("RGB", (10, 10)).save(path)
    plan = plan_for([m.insert_image_after_anchor(m.paragraph_anchor("Title"), path,
                                                 width_pt=30, height_pt=20, operation_id="chart")])
    image = paragraph("")
    image["paragraph"]["elements"].insert(0, {"inlineObjectElement": {"inlineObjectId": "image1"}})
    props = {"image1": {"inlineObjectProperties": {"embeddedObject": {"size": {
        "width": {"magnitude": 30, "unit": "PT"}, "height": {"magnitude": 20, "unit": "PT"}}}}}}
    after = document([paragraph("Title", style="HEADING_1"), image, paragraph("Analysis"), paragraph("")], revision="r2", images=props)
    client, api = client_with(document(), document(), after)
    api.batchUpdate.return_value.execute.side_effect = None
    api.batchUpdate.return_value.execute.return_value = {"writeControl": {"requiredRevisionId": "r2"},
                                                       "replies": [{"insertInlineImage": {"objectId": "image1"}}]}
    stager = Mock()
    stager.stage.return_value = "https://example.test/image.png"
    result = apply(client, plan, tmp_path, image_stager=stager)
    assert result["ok"] and result["objects"] == {"chart": "image1"}
    stager.cleanup.assert_called_once()
    verify_client, _ = client_with(after)
    assert verify_client.verify_mutation_plan(plan, receipt=result)["verified"]
    assert m.find_inline_object_after_heading(after, m.paragraph_anchor("Title")) == {"kind": "image", "object_id": "image1"}
    bad = deepcopy(after)
    bad["tabs"][0]["documentTab"]["inlineObjects"]["image1"]["inlineObjectProperties"]["embeddedObject"]["size"]["width"]["magnitude"] = 99
    verify_client, _ = client_with(bad)
    assert not verify_client.verify_mutation_plan(plan, receipt=result)["verified"]


def test_cli_preview_no_auth_no_body_stdout(tmp_path, monkeypatch, capsys):
    plan = plan_for([insert_op()])
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan.preview()))
    auth = Mock(side_effect=AssertionError("must not authenticate"))
    monkeypatch.setattr(cli.DocsClient, "from_oauth_file", auth)
    assert cli.main(["--token", "missing", "--expected-email", "a@b.test", "apply", "--plan", str(path)]) == 0
    output = capsys.readouterr().out
    assert "finding" not in output and "Analysis" not in output
    auth.assert_not_called()


def test_cli_get_multitab_and_plan_private_files(tmp_path, monkeypatch, capsys):
    d = document()
    d["tabs"].append(document(tab="t.second")["tabs"][0])
    client, api = client_with(d, d)
    monkeypatch.setattr(cli.DocsClient, "from_oauth_file", Mock(return_value=client))
    prefix = ["--token", "token", "--expected-email", "a@b.test"]
    output = tmp_path / "snapshot.json"
    assert cli.main([*prefix, "get", "doc1", "--output", str(output)]) == 0
    assert len(json.loads(output.read_text())["document"]["tabs"]) == 2
    assert "Analysis" not in capsys.readouterr().out
    ops = tmp_path / "ops.json"
    ops.write_text(json.dumps([insert_op()]))
    saved = tmp_path / "plan.json"
    assert cli.main([*prefix, "plan", "doc1", "--tab-id", "t.0", "--operations", str(ops), "--output", str(saved)]) == 0
    assert "finding" not in capsys.readouterr().out
    assert saved.stat().st_mode & 0o777 == 0o600
    api.batchUpdate.assert_not_called()


def test_cli_apply_and_verify_use_receipt(tmp_path, monkeypatch, capsys):
    plan = plan_for([insert_op()])
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan.preview()))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"test": "receipt"}))
    client = Mock()
    client.apply_mutation_plan.return_value = {"ok": True, "verified": True}
    client.verify_mutation_plan.return_value = {"ok": False, "verified": False}
    monkeypatch.setattr(cli.DocsClient, "from_oauth_file", Mock(return_value=client))
    prefix = ["--token", "token", "--expected-email", "a@b.test"]
    assert cli.main([*prefix, "apply", "--plan", str(path), "--apply", "--approved-digest", plan.digest,
                     "--receipt", str(receipt)]) == 0
    client.apply_mutation_plan.assert_called_once_with(plan, apply=True, approved_digest=plan.digest,
                                                       receipt_path=receipt, image_stager=None)
    assert cli.main([*prefix, "verify", "--plan", str(path), "--receipt", str(receipt)]) == 1
    client.verify_mutation_plan.assert_called_once_with(plan, receipt={"test": "receipt"})
    capsys.readouterr()


def test_schema_requests_are_valid_google_field_names():
    from googleapiclient.discovery_cache import get_static_doc
    schemas = json.loads(get_static_doc("docs", "v1"))["schemas"]
    values = [["a", "b"], ["c", "d"]]
    p = plan_for([m.insert_table_after_anchor(m.paragraph_anchor("Title"), values, operation_id="table",
                                             column_widths_pt=[60, 90], header_rows=[0])]).to_dict()
    d = document([paragraph("Title", style="HEADING_1"), paragraph(""), table(values), paragraph("Analysis"), paragraph("")])
    for req in m._compile(p["stages"][0], d, "t.0", "styles"):
        kind, fields = next(iter(req.items()))
        schema = schemas["Request"]["properties"][kind]["$ref"]
        assert set(fields) <= set(schemas[schema]["properties"])


def test_child_tab_edits_do_not_mark_parent_changed(tmp_path):
    original = document(tab="t.parent")
    original["tabs"][0]["childTabs"] = [document()["tabs"][0]]
    plan = m.plan_mutations(original, operations=[insert_op()], tab_id="t.0")
    after = deepcopy(original)
    after["revisionId"] = "r2"
    after["tabs"][0]["childTabs"] = [document([paragraph("Title", style="HEADING_1"),
        paragraph("Comment", style="HEADING_2"), paragraph("😀 finding", bullet=True),
        paragraph("Analysis"), paragraph("")])["tabs"][0]]
    client, _ = client_with(original, original, after, revisions=["r2"])
    assert apply(client, plan, tmp_path)["verified"]


def test_existing_table_row_styles_preserve_values():
    d = document([paragraph("Title"), table([["Name", "Value"], ["x", "1"]]), paragraph("")])
    t = m.get_normalized_outline(d)[1]
    op = m.style_table_rows({"kind": "table", "fingerprint": t["fingerprint"]}, [0], operation_id="header",
                             cell_style={"backgroundColor": m._color("#ffffff")}, text_style={"bold": True}, alignment="CENTER")
    p = plan_for([op], d).to_dict()
    requests = m._compile(p["stages"][0], d, "t.0", "write")
    assert not any("insertText" in r or "deleteContentRange" in r for r in requests)
    assert len([r for r in requests if "updateTableCellStyle" in r]) == 2
    assert all(r["updateTextStyle"]["fields"] == "bold" for r in requests if "updateTextStyle" in r)


def test_text_replace_utf16_and_deletion():
    d = document([paragraph("😀 value here"), paragraph("")])
    p = plan_for([m.replace_text(m.paragraph_anchor("😀 value here"), "value", "", operation_id="delete")], d).to_dict()
    request = m._compile(p["stages"][0], d, "t.0", "write")[0]
    assert request["deleteContentRange"]["range"] == {"tabId": "t.0", "startIndex": 4, "endIndex": 9}
    assert p["after"][0]["text"] == "😀  here\n"


def test_inline_insert_and_overlapping_match_rejection():
    p = plan_for([{"id": "inline", "operation": "insert_text", "anchor": m.paragraph_anchor("Title"),
                   "text": " addition"}]).to_dict()
    requests = m._compile(p["stages"][0], document(), "t.0", "write")
    assert requests == [{"insertText": {"location": {"tabId": "t.0", "index": 6}, "text": " addition"}}]
    d = document([paragraph("aaa"), paragraph("")])
    with pytest.raises(DocsEditError, match="one single-line"):
        plan_for([m.replace_text(m.paragraph_anchor("aaa"), "aa", "b", operation_id="x")], d)


def test_style_only_does_not_recreate_existing_bullets():
    d = document([paragraph("Title", bullet=True), paragraph("")])
    p = plan_for([{"id": "bold", "operation": "style_paragraph", "anchor": m.paragraph_anchor("Title"),
                   "text_style": {"bold": True}}], d).to_dict()
    assert not any("deleteParagraphBullets" in r or "createParagraphBullets" in r
                   for r in m._compile(p["stages"][0], d, "t.0", "write"))


def test_empty_bullets_and_unsupported_move_are_rejected():
    with pytest.raises(DocsEditError, match="Empty bullet"):
        plan_for([m.insert_bullets_after_anchor(m.paragraph_anchor("Title"), [""], operation_id="empty")])
    d = document([paragraph("Title"), paragraph("Analysis", bullet=True), paragraph("")])
    with pytest.raises(DocsEditError, match="non-bullet"):
        plan_for([m.move_paragraph_block(m.paragraph_anchor("Analysis"), m.paragraph_anchor("Analysis"),
                                         m.paragraph_anchor("Title"), operation_id="move", position="before")], d)


def test_missing_image_receipt_cannot_verify(tmp_path):
    from PIL import Image
    path = tmp_path / "image.jpg"
    Image.new("RGB", (4, 4)).save(path)
    plan = plan_for([m.insert_image_after_anchor(m.paragraph_anchor("Title"), path, width_pt=5, height_pt=5, operation_id="i")])
    client, _ = client_with(document())
    assert not client.verify_mutation_plan(plan)["verified"]


def test_complete_report_example_offline(tmp_path):
    import importlib.util
    from pathlib import Path
    from PIL import Image
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("report_example", root / "examples/docs_report_mutations.py")
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    config = json.loads((root / "examples/docs_report_config.json").read_text())
    image_path = tmp_path / "chart.png"
    Image.new("RGB", (20, 10)).save(image_path)
    config["chart"]["path"] = str(image_path)
    blocks, images = [], {}

    def image_block(oid, width=40, height=20):
        block = paragraph("")
        block["paragraph"]["elements"].insert(0, {"inlineObjectElement": {"inlineObjectId": oid}})
        images[oid] = {"inlineObjectProperties": {"embeddedObject": {"size": {
            "width": {"magnitude": width, "unit": "PT"}, "height": {"magnitude": height, "unit": "PT"}},
            "imageProperties": {"contentUri": "https://example.test/temporary-read-url"}}}}
        return block

    for i, item in enumerate(config["image_comments"]):
        blocks.extend([paragraph(item["heading"]["text"], style="HEADING_2"), image_block(f"old-{i}")])
    blocks.extend([paragraph("NEWS", style="HEADING_2"), paragraph("Analysis"), paragraph("")])
    original = document(blocks, images=images)
    plan = plan_for(example.report_operations(original, config, tab_id="t.0"), original)
    snapshots, replies = [], []
    for i, item in enumerate(config["image_comments"]):
        at = 4 * i + 2
        blocks[at:at] = [paragraph(item["title"], style="HEADING_3"), paragraph(item["bullets"][0], bullet=True)]
        revision = f"r{len(snapshots) + 2}"
        snapshots.append(document(blocks, images=images, revision=revision))
        replies.append({"writeControl": {"requiredRevisionId": revision}})
    blocks.insert(13, image_block("new-chart", 360, 180))
    snapshots.append(document(blocks, images=images, revision="r5"))
    replies.append({"writeControl": {"requiredRevisionId": "r5"}, "replies": [{"insertInlineImage": {"objectId": "new-chart"}}]})
    blocks[14:14] = [paragraph(""), table([[""] * 6 for _ in range(6)])]
    snapshots.append(document(blocks, images=images, revision="r6"))
    blocks[15] = table(config["table_values"])
    snapshots.append(document(blocks, images=images, revision="r7"))
    blocks[15] = table(config["table_values"], styled=True)
    blocks[15]["table"]["tableStyle"] = {"tableColumnProperties": [
        {"widthType": "FIXED_WIDTH", "width": {"magnitude": w, "unit": "PT"}} for w in config["column_widths_pt"]]}
    snapshots.append(document(blocks, images=images, revision="r8"))
    replies.extend({"writeControl": {"requiredRevisionId": f"r{r}"}} for r in (6, 7, 8))
    client, api = client_with(original, original, *snapshots)
    api.batchUpdate.return_value.execute.side_effect = replies
    stager = Mock()
    stager.stage.return_value = "https://example.test/chart.png"
    result = apply(client, plan, tmp_path, image_stager=stager)
    assert result["ok"], result
    assert len(result["operations"]) == 5 and len(result["stages"]) == 7
    verify_client, _ = client_with(snapshots[-1])
    assert verify_client.verify_mutation_plan(plan, receipt=result)["verified"]
    assert m.get_normalized_outline(snapshots[-1])[-2]["text"] == "Analysis\n"


def test_expiring_image_content_url_is_not_a_content_change():
    d = document()
    block = paragraph("")
    block["paragraph"]["elements"].insert(0, {"inlineObjectElement": {"inlineObjectId": "i"}})
    d = document([block, paragraph("")], images={"i": {"inlineObjectProperties": {
        "embeddedObject": {"imageProperties": {"contentUri": "url-one"}}}}})
    before = m.get_normalized_outline(d)
    d["tabs"][0]["documentTab"]["inlineObjects"]["i"]["inlineObjectProperties"]["embeddedObject"]["imageProperties"]["contentUri"] = "url-two"
    assert before == m.get_normalized_outline(d)


def test_api_read_errors_never_expose_body():
    client, api = client_with()
    api.get.return_value.execute.side_effect = RuntimeError("secret token PRIVATE BODY")
    with pytest.raises(DocsEditError) as caught:
        client.plan_mutations("doc1", operations=[insert_op()])
    assert "secret" not in str(caught.value) and "PRIVATE" not in str(caught.value)


def test_table_conflict_stops_after_creation(tmp_path):
    plan = plan_for([m.insert_table_after_anchor(m.paragraph_anchor("Title"), [["a", "b"]], operation_id="table")])
    first = document([paragraph("Title", style="HEADING_1"), paragraph(""), table([["", ""]]),
                      paragraph("Analysis"), paragraph("")], revision="changed-by-collaborator")
    client, api = client_with(document(), document(), first, revisions=["r2"])
    result = apply(client, plan, tmp_path)
    assert result["write_status"] == "partial" and not result["ok"]
    assert api.batchUpdate.call_count == 1


def test_style_heading_does_not_send_readonly_heading_id():
    d = document()
    d["tabs"][0]["documentTab"]["body"]["content"][0]["paragraph"]["paragraphStyle"]["headingId"] = "h.123"
    p = plan_for([{"id": "style", "operation": "style_paragraph", "anchor": m.paragraph_anchor("Title"),
                   "paragraph_style": {"alignment": "CENTER"}}], d).to_dict()
    requests = m._compile(p["stages"][0], d, "t.0", "write")
    assert requests == [{"updateParagraphStyle": {"range": {"tabId": "t.0", "startIndex": 1, "endIndex": 7},
                                                  "paragraphStyle": {"alignment": "CENTER"}, "fields": "alignment"}}]

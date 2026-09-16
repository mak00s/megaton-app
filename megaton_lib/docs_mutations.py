"""Reviewed report-body mutations. Plans/receipts are confidential artifacts.

Only top-level paragraphs, inline images and unmerged rectangular tables are
editable. Unknown structures can be retained/anchored, never silently flattened.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path

from .docs_client import DocsEditError, _has_suggestions, _id, _select, _tab_id, _tabs

SCHEMA = "docs-mutations/v1"
RECEIPT_SCHEMA = "docs-mutation-receipt/v1"
PARAGRAPH_FIELDS = {"namedStyleType", "alignment", "spaceAbove", "spaceBelow",
                    "indentStart", "indentEnd", "indentFirstLine", "lineSpacing", "keepWithNext"}
TEXT_FIELDS = {"bold", "italic", "underline", "strikethrough", "fontSize", "foregroundColor", "backgroundColor", "link",
               "weightedFontFamily", "smallCaps", "baselineOffset"}
BOOLEAN_TEXT_FIELDS = {"bold", "italic", "underline", "strikethrough", "smallCaps"}
CELL_FIELDS = {"backgroundColor", "contentAlignment", "paddingTop", "paddingBottom", "paddingLeft", "paddingRight"}


def _read_document(client, document_id):
    try:
        return client.get(document_id)
    except Exception:
        raise DocsEditError("Document read failed; check explicit OAuth/access and any existing receipt.") from None


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def utf16_length(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _text(value, *, empty=False):
    if not isinstance(value, str) or (not value and not empty):
        raise DocsEditError("Expected text; empty text is not allowed here.")
    if any((ord(c) < 32 and c != "\n") or 0xD800 <= ord(c) <= 0xDFFF or
           0xE000 <= ord(c) <= 0xF8FF or c in "\x85\u2028\u2029" for c in value):
        raise DocsEditError("Unsupported control/private-use character in text.")
    return value


def _semantic(value):
    # Indices and generated list IDs change after insertion, but not content.
    if isinstance(value, dict):
        return {k: _semantic(v) for k, v in value.items()
                if k not in {"startIndex", "endIndex", "listId", "contentUri"}}
    if isinstance(value, list):
        return [_semantic(v) for v in value]
    return value


def _other_tabs(document, tab_id):
    # Parent tabs contain childTabs. Hash their own contents, not the edited child.
    return {t["tabProperties"]["tabId"]: _digest(_semantic({k: v for k, v in t.items() if k != "childTabs"}))
            for t in _tabs(document) if t["tabProperties"]["tabId"] != tab_id}


def _subset(expected, actual, *, tolerance=1e-6):
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(k in actual and _subset(v, actual[k], tolerance=0.05 if k == "magnitude" else 1e-6)
                                                for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(expected) == len(actual) and all(_subset(a, b) for a, b in zip(expected, actual))
    if isinstance(expected, (float, int)) and not isinstance(expected, bool):
        return isinstance(actual, (float, int)) and math.isclose(expected, actual, abs_tol=tolerance, rel_tol=0)
    return expected == actual


def _style(value, allowed):
    if not isinstance(value, dict) or not set(value) <= allowed or not value:
        raise DocsEditError("Unsupported or empty style fields.")
    _json(value)
    return deepcopy(value)


def _para(block, tab):
    p = block["paragraph"]
    runs = p.get("elements", [])
    text = "".join(e.get("textRun", {}).get("content", "") for e in runs)
    bullet = p.get("bullet")
    styles = {"namedStyleType": "NORMAL_TEXT", **p.get("paragraphStyle", {})}
    result = {"kind": "paragraph", "text": text, "paragraph_style": styles,
              "bullet": bool(bullet), "runs": []}
    for e in runs:
        if "textRun" in e:
            result["runs"].append({"text": e["textRun"]["content"],
                                   "style": e["textRun"].get("textStyle", {})})
    if any(not ({"textRun", "inlineObjectElement"} & set(e)) for e in runs):
        result["kind"] = "opaque"
    if bullet:
        props = tab.get("lists", {}).get(bullet.get("listId"), {}).get("listProperties", {})
        result["bullet_properties"] = {"level": bullet.get("nestingLevel", 0), "list": props}
    images = [e["inlineObjectElement"]["inlineObjectId"] for e in runs if "inlineObjectElement" in e]
    if images:
        result.update(kind="image" if len(images) == 1 and text == "\n" else "mixed", objects=images)
        result["images"] = [_semantic(tab.get("inlineObjects", {}).get(i, {}).get("inlineObjectProperties", {})) for i in images]
    return result


def _nodes(tab):
    body = tab["documentTab"]
    result = []
    for block in body.get("body", {}).get("content", []):
        if "paragraph" in block:
            node = _para(block, body)
        elif "table" in block:
            table = block["table"]
            cells = []
            for row in table.get("tableRows", []):
                cells.append([{"paragraphs": [_para(p, body) for p in c.get("content", []) if "paragraph" in p],
                               "style": c.get("tableCellStyle", {})} for c in row.get("tableCells", [])])
            node = {"kind": "table", "rows": table.get("rows"), "columns": table.get("columns"),
                    "cells": cells, "table_style": table.get("tableStyle", {}),
                    "editable": all(len(row.get("tableCells", [])) == table.get("columns") and all(
                        c.get("tableCellStyle", {}).get("rowSpan", 1) == 1 and
                        c.get("tableCellStyle", {}).get("columnSpan", 1) == 1 and
                        all("paragraph" in p and all("textRun" in e for e in p["paragraph"].get("elements", []))
                            for p in c.get("content", [])) for c in row.get("tableCells", []))
                                    for row in table.get("tableRows", []))}
        else:
            node = {"kind": "opaque"}
        semantic = _semantic(block)
        # Include image/list properties, which live outside the paragraph body.
        node["fingerprint"] = _digest([semantic, node])
        node["start"] = block.get("startIndex", 0)
        node["end"] = block.get("endIndex", 0)
        node["raw"] = block
        result.append(node)
    return result


def get_normalized_outline(document: dict, *, tab_id=None) -> list[dict]:
    """Confidential outline including tables/images; never print to ordinary logs."""
    return [{k: v for k, v in n.items() if k != "raw"} for n in _nodes(_select(document, tab_id))]


def paragraph_anchor(text: str, *, named_style_type=None, within_heading=None) -> dict:
    result = {"kind": "paragraph", "text": text}
    if named_style_type:
        result["named_style_type"] = named_style_type
    if within_heading:
        result["within_heading"] = within_heading
    return result


def _find(nodes, anchor):
    if not isinstance(anchor, dict):
        raise DocsEditError("Expected an anchor object.")
    lower, upper = 0, len(nodes)
    if anchor.get("within_heading"):
        lower = _find(nodes, anchor["within_heading"]) + 1
        heading = nodes[lower - 1].get("paragraph_style", {}).get("namedStyleType", "")
        if not heading.startswith("HEADING_"):
            raise DocsEditError("Section anchor must be a heading.")
        upper = next((i for i in range(lower, len(nodes)) if
                      nodes[i].get("paragraph_style", {}).get("namedStyleType", "").startswith("HEADING_") and
                      nodes[i]["paragraph_style"]["namedStyleType"] <= heading), len(nodes))
    matches = []
    for i in range(lower, upper):
        n = nodes[i]
        if anchor.get("operation_id"):
            yes = n.get("operation_id") == anchor["operation_id"]
        elif anchor.get("kind") == "image":
            yes = n["kind"] == "image" and n.get("objects") == [anchor.get("object_id")]
        elif anchor.get("kind") == "table":
            yes = n["kind"] == "table" and n.get("fingerprint") == anchor.get("fingerprint")
        else:
            yes = (n["kind"] == "paragraph" and n["text"].removesuffix("\n") == anchor.get("text") and
                   (not anchor.get("named_style_type") or
                    n.get("paragraph_style", {}).get("namedStyleType") == anchor["named_style_type"]))
        if yes:
            matches.append(i)
    if len(matches) != 1:
        raise DocsEditError(f"Expected one anchor match; found {len(matches)}.")
    return matches[0]


def find_unique_paragraph(document, text, *, tab_id=None, named_style_type=None, within_heading=None):
    nodes = get_normalized_outline(document, tab_id=tab_id)
    return nodes[_find(nodes, paragraph_anchor(text, named_style_type=named_style_type, within_heading=within_heading))]


def find_inline_object_after_heading(document, heading, *, tab_id=None):
    nodes = get_normalized_outline(document, tab_id=tab_id)
    i = _find(nodes, heading) + 1
    while i < len(nodes) and nodes[i]["kind"] == "paragraph" and nodes[i]["text"] == "\n":
        i += 1
    if i == len(nodes) or nodes[i]["kind"] != "image":
        raise DocsEditError("Expected a standalone inline image after the heading.")
    return {"kind": "image", "object_id": nodes[i]["objects"][0]}


def insert_paragraphs_after_anchor(anchor, paragraphs, *, operation_id):
    return {"id": operation_id, "operation": "insert_paragraphs", "anchor": anchor, "position": "after", "paragraphs": paragraphs}


def insert_bullets_after_anchor(anchor, items, *, operation_id):
    return insert_paragraphs_after_anchor(anchor, [{"text": s, "bullet": True} for s in items], operation_id=operation_id)


def insert_image_after_anchor(anchor, image_path, *, width_pt, height_pt, operation_id):
    return {"id": operation_id, "operation": "insert_image", "anchor": anchor, "position": "after",
            "image_path": str(image_path), "width_pt": width_pt, "height_pt": height_pt}


def insert_table_after_anchor(anchor, values, *, operation_id, column_widths_pt=None,
                              header_rows=(), total_rows=(), header_color="#e8eaed", total_color="#e8eaed"):
    return {"id": operation_id, "operation": "insert_table", "anchor": anchor, "position": "after",
            "values": values, "column_widths_pt": column_widths_pt,
            "header_rows": list(header_rows), "total_rows": list(total_rows),
            "header_color": header_color, "total_color": total_color}


def move_paragraph_block(first, last, destination, *, operation_id, position="after"):
    return {"id": operation_id, "operation": "move_paragraphs", "anchor": first,
            "last": last, "destination": destination, "position": position}


def style_table_rows(anchor, rows, *, operation_id, cell_style, text_style=None, alignment=None):
    return {"id": operation_id, "operation": "style_table_rows", "anchor": anchor,
            "rows": rows, "cell_style": cell_style, "text_style": text_style or {}, "alignment": alignment}


def replace_text(anchor, match, text, *, operation_id):
    """Replace one literal within a text paragraph; empty replacement deletes it."""
    return {"id": operation_id, "operation": "replace_text", "anchor": anchor, "match": match, "text": text}


def _paragraph(spec):
    if isinstance(spec, str):
        spec = {"text": spec}
    if not isinstance(spec, dict) or set(spec) - {"text", "paragraph_style", "text_style", "bullet"}:
        raise DocsEditError("Invalid paragraph specification.")
    text = _text(spec.get("text", ""), empty=True)
    if "\n" in text:
        raise DocsEditError("Supply each paragraph separately, without newline terminators.")
    if spec.get("bullet") and not text.strip():
        raise DocsEditError("Empty bullet paragraphs are not supported.")
    ps = {"namedStyleType": "NORMAL_TEXT", **spec.get("paragraph_style", {})}
    ts = deepcopy(spec.get("text_style", {}))
    _style(ps, PARAGRAPH_FIELDS)
    if ts:
        _style(ts, TEXT_FIELDS)
    return {"kind": "paragraph", "text": text + "\n", "paragraph_style": ps,
            "text_style": ts, "text_style_mode": "reset", "bullet": bool(spec.get("bullet", False))}


def _validate_insertion_location(nodes, at):
    if at == len(nodes) or nodes[at]["kind"] == "opaque":
        raise DocsEditError("Insertion needs an existing body paragraph; use before the final paragraph.")
    if nodes[at]["kind"] == "table":
        raise DocsEditError('Cannot insert at a table start. Select its preceding paragraph with position="before"; review the changed location.')


def _color(hex_value):
    import re
    if not isinstance(hex_value, str) or not re.fullmatch(r"#[a-fA-F0-9]{6}", hex_value):
        raise DocsEditError("Colors must use #RRGGBB.")
    return {"color": {"rgbColor": {k: int(hex_value[i:i+2], 16) / 255
                                     for k, i in zip(("red", "green", "blue"), (1, 3, 5))}}}


def _dimension(value):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
        raise DocsEditError("Dimensions must be finite positive points.")
    return {"magnitude": value, "unit": "PT"}


def _image_info(path):
    from PIL import Image
    p = Path(path).expanduser().resolve()
    data = p.read_bytes()
    try:
        with Image.open(p) as image:
            if image.format not in {"PNG", "JPEG"} or image.width * image.height > 25_000_000 or len(data) >= 50_000_000:
                raise DocsEditError("Unsupported image format or size.")
            image.verify()
    except DocsEditError:
        raise
    except Exception:
        raise DocsEditError("Invalid local image.") from None
    return {"path": str(p), "sha256": hashlib.sha256(data).hexdigest()}


def _expected(nodes):
    return [{k: deepcopy(v) for k, v in n.items() if k not in {"raw", "start", "end"}} for n in nodes]


@dataclass(frozen=True)
class DocsMutationPlan:
    """Immutable canonical payload; digest must also be retained at approval time."""
    payload_json: str

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA:
            raise DocsEditError("Unsupported mutation plan schema.")
        value = deepcopy(value)
        digest = value.pop("digest", None)
        if digest != _digest(value):
            raise DocsEditError("Mutation plan digest mismatch.")
        required = {"schema_version", "document_id", "tab_id", "revision_id", "operations", "stages", "before", "after", "other_tabs", "assets", "context"}
        if set(value) != required:
            raise DocsEditError("Invalid mutation plan fields.")
        return cls(_json({**value, "digest": digest}))

    def to_dict(self):
        return json.loads(self.payload_json)

    @property
    def digest(self):
        return self.to_dict()["digest"]

    def preview(self):
        p = self.to_dict()
        return {"schema_version": SCHEMA, "mode": "dry_run", "ok": True, "verified": False,
                "write_status": "not_started", "plan": p,
                "summary": [{"operation_id": s["id"], "operation": s["operation"], "range": s["range"],
                             "write_phases": s["phases"]} for s in p["stages"]]}


def plan_mutations(document, *, operations, tab_id=None):
    tab = _select(document, tab_id)
    tid = _tab_id(tab["tabProperties"]["tabId"])
    if not document.get("revisionId") or _has_suggestions(tab):
        raise DocsEditError("An editable snapshot without suggestions is required.")
    if not isinstance(operations, list) or not operations:
        raise DocsEditError("Provide a nonempty operation list.")
    nodes = _expected(_nodes(tab))
    before = deepcopy(nodes)
    stages, assets, seen = [], [], set()
    for op in deepcopy(operations):
        oid = _id(op.get("id"))
        if oid in seen:
            raise DocsEditError("Operation IDs must be unique.")
        seen.add(oid)
        i = _find(nodes, op.get("anchor"))
        kind = op.get("operation")
        position = op.get("position", "after")
        if position not in {"before", "after"}:
            raise DocsEditError("Position must be before or after.")
        stage = {"id": oid, "operation": kind, "before": deepcopy(nodes), "index": i,
                 "range": [i, i + 1], "phases": ["write"], "anchor_before": deepcopy(nodes[i - 1]) if i else None,
                 "anchor_after": deepcopy(nodes[i + 1]) if i + 1 < len(nodes) else None}
        if kind in {"insert_paragraphs", "insert_image", "insert_table"}:
            at = i + (position == "after")
            # Inserting after the final paragraph needs special terminal-newline
            # handling. Require an explicit existing trailing empty paragraph.
            _validate_insertion_location(nodes, at)
            stage["at"] = at
            if kind == "insert_paragraphs":
                new = [_paragraph(s) for s in op.get("paragraphs", [])]
                if not new:
                    raise DocsEditError("Provide paragraphs to insert.")
                for n in new:
                    n["operation_id"] = oid
            elif kind == "insert_image":
                asset = _image_info(op["image_path"])
                asset["operation_id"] = oid
                assets.append(asset)
                new = [{"kind": "image", "objects": ["$" + oid], "text": "\n", "bullet": False,
                        "paragraph_style": {"namedStyleType": "NORMAL_TEXT"}, "operation_id": oid,
                        "size": {"width": _dimension(op["width_pt"]), "height": _dimension(op["height_pt"])}}]
            else:
                values = op.get("values")
                if not isinstance(values, list) or not values or not isinstance(values[0], list) or not values[0]:
                    raise DocsEditError("Provide a nonempty rectangular table.")
                width = len(values[0])
                if any(not isinstance(row, list) or len(row) != width for row in values):
                    raise DocsEditError("Table rows must have equal lengths.")
                cells = [[{"paragraphs": [_paragraph(s) for s in _text(v, empty=True).split("\n")], "style": {}}
                          for v in row] for row in values]
                widths = op.get("column_widths_pt")
                if widths is not None and (not isinstance(widths, list) or len(widths) != width):
                    raise DocsEditError("Column width count must match the table.")
                for w in widths or []:
                    _dimension(w)
                for name in ("header", "total"):
                    for row in op.get(name + "_rows", []):
                        if type(row) is not int or not 0 <= row < len(cells):
                            raise DocsEditError("Invalid styled table row.")
                        for c in cells[row]:
                            c["style"]["backgroundColor"] = _color(op.get(name + "_color", "#e8eaed"))
                            for p in c["paragraphs"]:
                                p["text_style"]["bold"] = True
                new = [_paragraph(""), {"kind": "table", "rows": len(cells), "columns": width,
                                        "cells": cells, "widths": widths, "operation_id": oid}]
                stage["phases"] = ["table", "values", "styles"]
            nodes[at:at] = new
            stage["new"] = deepcopy(new)
        elif kind in {"delete_paragraphs", "replace_paragraphs", "move_paragraphs"}:
            last = _find(nodes, op.get("last", op["anchor"]))
            if last < i or last == len(nodes) - 1 or any(n["kind"] != "paragraph" for n in nodes[i:last + 1]):
                raise DocsEditError("Range must contain body paragraphs, excluding the final paragraph.")
            stage["range"] = [i, last + 1]
            if kind == "move_paragraphs":
                dest = _find(nodes, op["destination"]) + (position == "after")
                if i <= dest <= last + 1 or dest == len(nodes):
                    raise DocsEditError("Invalid move destination.")
                _validate_insertion_location(nodes, dest)
                new = [_movable(n) for n in nodes[i:last + 1]]
                stage["destination"] = dest
                del nodes[i:last + 1]
                at = dest - (last + 1 - i) if dest > last else dest
                nodes[at:at] = deepcopy(new)
                stage["at"] = at
            else:
                new = [] if kind == "delete_paragraphs" else [_paragraph(s) for s in op["paragraphs"]]
                nodes[i:last + 1] = deepcopy(new)
            stage["new"] = new
        elif kind == "style_paragraph":
            n = nodes[i]
            if n["kind"] != "paragraph":
                raise DocsEditError("Style target must be a text paragraph.")
            n.pop("fingerprint", None)
            n.setdefault("text_style_mode", "patch")
            if op.get("paragraph_style"):
                n["paragraph_style"].update(_style(op["paragraph_style"], PARAGRAPH_FIELDS))
                if "namedStyleType" in op["paragraph_style"]:
                    n["paragraph_style"].pop("headingId", None)
            if op.get("text_style"):
                n.setdefault("text_style", {}).update(_style(op["text_style"], TEXT_FIELDS))
            if "bullet" in op:
                if type(op["bullet"]) is not bool or (op["bullet"] and not n["text"].strip()):
                    raise DocsEditError("Invalid bullet specification.")
                n["bullet"] = op["bullet"]
                n.pop("bullet_properties", None)
            stage["change_bullets"] = "bullet" in op
            stage["paragraph_patch"] = op.get("paragraph_style", {})
            stage["text_patch"] = op.get("text_style", {})
        elif kind in {"replace_text", "insert_text"}:
            n = nodes[i]
            match = _text(op.get("match", n.get("text", "").removesuffix("\n")), empty=kind == "insert_text")
            text = _text(op.get("text"), empty=kind == "replace_text")
            offsets = [j for j in range(len(n.get("text", ""))) if n["text"].startswith(match, j)] if match else [0]
            if "\n" in match or "\n" in text or n["kind"] != "paragraph" or len(offsets) != 1:
                raise DocsEditError("Expected one single-line match inside a text paragraph.")
            new = _movable(n)
            # Inline edits retain the source run style; they do not rebuild a paragraph.
            new["text_style_mode"] = "preserve"
            offset = offsets[0] + (len(match) if kind == "insert_text" and position == "after" else 0)
            length = len(match) if kind == "replace_text" else 0
            new["text"] = n["text"][:offset] + text + n["text"][offset + length:]
            stage.update(offset=utf16_length(n["text"][:offset]), delete_length=utf16_length(match) if length else 0, text=text)
            nodes[i] = new
        elif kind == "style_table_rows":
            n = nodes[i]
            if n["kind"] != "table" or not n.get("editable", "fingerprint" not in n):
                raise DocsEditError("Row style target must be a table.")
            rows = op.get("rows")
            if not isinstance(rows, list) or not rows or any(type(r) is not int or not 0 <= r < n["rows"] for r in rows):
                raise DocsEditError("Invalid table row selection.")
            cs = _style(op.get("cell_style"), CELL_FIELDS)
            ts = _style(op["text_style"], TEXT_FIELDS) if op.get("text_style") else {}
            n.pop("fingerprint", None)
            for r in rows:
                for c in n["cells"][r]:
                    if c["style"].get("rowSpan", 1) != 1 or c["style"].get("columnSpan", 1) != 1:
                        raise DocsEditError("Merged table cells are unsupported.")
                    c["style"].update(cs)
                    for paragraph in c["paragraphs"]:
                        paragraph.setdefault("text_style_mode", "patch")
                        paragraph.setdefault("text_style", {}).update(ts)
                        if op.get("alignment"):
                            paragraph["paragraph_style"]["alignment"] = op["alignment"]
            stage.update(rows=rows, cell_style=cs, text_style=ts, alignment=op.get("alignment"))
        else:
            raise DocsEditError("Unsupported mutation operation.")
        stage["after"] = deepcopy(nodes)
        stages.append(stage)
    payload = {"schema_version": SCHEMA, "document_id": _id(document["documentId"]), "tab_id": tid,
               "revision_id": document["revisionId"], "operations": deepcopy(operations),
               "stages": stages, "before": before, "after": nodes, "assets": assets,
               "other_tabs": _other_tabs(document, tid), "context": _context(document, tab)}
    return DocsMutationPlan.from_dict({**payload, "digest": _digest(payload)})


def _movable(node):
    # Moves deliberately reject structure we cannot faithfully recreate.
    if node.get("bullet") or node.get("kind") != "paragraph" or not node.get("runs"):
        raise DocsEditError("Move supports non-bullet text paragraphs only.")
    if len(node["runs"]) != 1 or set(node["runs"][0]["style"]) - TEXT_FIELDS:
        raise DocsEditError("Move of mixed/unsupported text styles requires an explicit reviewed replacement.")
    ps = {k: v for k, v in node["paragraph_style"].items() if k != "headingId"}
    if set(ps) - PARAGRAPH_FIELDS:
        raise DocsEditError("Move of unsupported paragraph styles is not allowed.")
    return _paragraph({"text": node["text"].removesuffix("\n"), "paragraph_style": ps,
                       "text_style": node["runs"][0]["style"]})


def _node_matches(expected, actual, objects):
    if expected.get("fingerprint"):
        return expected["fingerprint"] == actual.get("fingerprint")
    if expected["kind"] != actual["kind"]:
        return False
    if expected["kind"] == "table":
        if (expected["rows"], expected["columns"]) != (actual["rows"], actual["columns"]):
            return False
        if len(expected["cells"]) != len(actual["cells"]):
            return False
        for er, ar in zip(expected["cells"], actual["cells"]):
            if len(er) != len(ar):
                return False
            for ec, ac in zip(er, ar):
                if not _subset(ec["style"], ac["style"]) or len(ec["paragraphs"]) != len(ac["paragraphs"]):
                    return False
                if not all(_node_matches(e, a, objects) for e, a in zip(ec["paragraphs"], ac["paragraphs"])):
                    return False
        widths = expected.get("widths")
        if widths:
            cols = actual["table_style"].get("tableColumnProperties", [])
            if len(cols) != len(widths) or not all(_subset({"widthType": "FIXED_WIDTH", "width": _dimension(w)}, c)
                                                   for w, c in zip(widths, cols)):
                return False
        return True
    if expected.get("text") != actual.get("text") or expected.get("bullet") != actual.get("bullet"):
        return False
    if "bullet_properties" in expected and not _subset(expected["bullet_properties"], actual.get("bullet_properties", {})):
        return False
    if not _subset(expected.get("paragraph_style", {}), actual.get("paragraph_style", {})):
        return False
    if expected["kind"] == "image":
        ids = [objects.get(i[1:]) if i.startswith("$") else i for i in expected["objects"]]
        return (None not in ids and ids == actual.get("objects") and
                _subset(expected["size"], actual["images"][0].get("embeddedObject", {}).get("size", {})))
    wanted = []
    for run in expected.get("runs", [{"text": expected["text"], "style": {}}]):
        wanted.extend([{**run["style"], **expected.get("text_style", {})}] * len(run["text"]))
    observed = []
    for run in actual.get("runs", []):
        observed.extend([{**dict.fromkeys(BOOLEAN_TEXT_FIELDS, False), **run["style"]}] * len(run["text"]))
    if len(wanted) != len(observed):
        return False
    for index, (e, a) in enumerate(zip(wanted, observed)):
        reset = expected.get("text_style_mode") == "reset"
        if not reset and expected["text"][index] == "\n":
            continue
        if not _subset(e, a):
            return False
        if reset and any(k not in e and not (k in BOOLEAN_TEXT_FIELDS and v is False) for k, v in a.items()):
            return False
    return True


def _check(document, plan, expected, objects):
    nodes = _nodes(_select(document, plan["tab_id"]))
    mismatch = []
    if _has_suggestions(_select(document, plan["tab_id"])):
        mismatch.append("suggestions_present")
    if len(nodes) != len(expected):
        mismatch.append("body_structure_mismatch")
    else:
        for i, (e, a) in enumerate(zip(expected, nodes)):
            if not _node_matches(e, a, objects):
                mismatch.append(f"block:{i}:{e['kind']}_mismatch")
    others = _other_tabs(document, plan["tab_id"])
    if others != plan["other_tabs"]:
        mismatch.append("outside_tab_changed")
    if _context(document, _select(document, plan["tab_id"])) != plan["context"]:
        mismatch.append("outside_body_context_changed")
    return mismatch


def _context(document, tab):
    return _digest({"tab": {k: _semantic(v) for k, v in tab["documentTab"].items()
                            if k not in {"body", "inlineObjects", "lists"}},
                    "document_style": document.get("documentStyle", {}),
                    "named_styles": document.get("namedStyles", {})})


def _range(tid, start, end):
    return {"tabId": tid, "startIndex": start, "endIndex": end}


def _insert(tid, index, text):
    return {"insertText": {"location": {"tabId": tid, "index": index}, "text": text}}


def _paragraph_requests(tid, start, paragraphs, *, insert=True, manage_bullets=True):
    requests = [_insert(tid, start, "".join(p["text"] for p in paragraphs))] if insert else []
    cursor = start
    for p in paragraphs:
        end = cursor + utf16_length(p["text"])
        rng = _range(tid, cursor, end)
        if manage_bullets:
            requests.append({"deleteParagraphBullets": {"range": rng}})
        ps = p.get("paragraph_style", {})
        if ps:
            requests.append({"updateParagraphStyle": {"range": rng, "paragraphStyle": ps, "fields": ",".join(ps)}})
        ts = p.get("text_style", {})
        reset = p.get("text_style_mode") == "reset"
        if reset or (ts and end > cursor + 1):
            requests.append({"updateTextStyle": {"range": _range(tid, cursor, end if reset else end - 1),
                            "textStyle": ts, "fields": ",".join(sorted(TEXT_FIELDS if reset else ts))}})
        if manage_bullets and p.get("bullet"):
            requests.append({"createParagraphBullets": {"range": rng, "bulletPreset": "BULLET_DISC_CIRCLE_SQUARE"}})
        cursor = end
    return requests


def _compile(stage, document, tid, phase, image_uri=None):
    nodes = _nodes(_select(document, tid))
    kind, i = stage["operation"], stage["index"]
    if kind == "insert_paragraphs":
        return _paragraph_requests(tid, nodes[stage["at"]]["start"], stage["new"])
    if kind in {"delete_paragraphs", "replace_paragraphs", "move_paragraphs"}:
        first, end = stage["range"]
        start, stop = nodes[first]["start"], nodes[end - 1]["end"]
        requests = [{"deleteContentRange": {"range": _range(tid, start, stop)}}]
        if kind == "move_paragraphs":
            destination = nodes[stage["destination"]]["start"]
            start = destination - (stop - start) if destination > stop else destination
        return requests + (_paragraph_requests(tid, start, stage["new"]) if stage["new"] else [])
    if kind == "style_paragraph":
        target = {**stage["after"][i], "paragraph_style": stage["paragraph_patch"], "text_style": stage["text_patch"],
                  "text_style_mode": "patch"}
        return _paragraph_requests(tid, nodes[i]["start"], [target], insert=False,
                                   manage_bullets=stage["change_bullets"])
    if kind in {"replace_text", "insert_text"}:
        start = nodes[i]["start"] + stage["offset"]
        requests = ([{"deleteContentRange": {"range": _range(tid, start, start + stage["delete_length"])}}]
                    if stage["delete_length"] else [])
        if stage["text"]:
            requests.append(_insert(tid, start, stage["text"]))
        return requests
    if kind == "style_table_rows":
        table = nodes[i]
        requests = []
        for r in stage["rows"]:
            for c, cell in enumerate(table["raw"]["table"]["tableRows"][r]["tableCells"]):
                requests.append({"updateTableCellStyle": {"tableRange": {
                    "tableCellLocation": {"tableStartLocation": {"tabId": tid, "index": table["start"]},
                                          "rowIndex": r, "columnIndex": c}, "rowSpan": 1, "columnSpan": 1},
                    "tableCellStyle": stage["cell_style"], "fields": ",".join(stage["cell_style"])}})
                for p in cell["content"]:
                    if "paragraph" not in p:
                        raise DocsEditError("Unsupported table cell content.")
                    rng = _range(tid, p["startIndex"], p["endIndex"])
                    if stage["text_style"]:
                        requests.append({"updateTextStyle": {"range": rng, "textStyle": stage["text_style"],
                                                              "fields": ",".join(stage["text_style"])}})
                    if stage["alignment"]:
                        requests.append({"updateParagraphStyle": {"range": rng,
                            "paragraphStyle": {"alignment": stage["alignment"]}, "fields": "alignment"}})
        return requests
    at = stage["at"]
    if kind == "insert_image":
        start = nodes[at]["start"]
        return [_insert(tid, start, "\n"),
                {"deleteParagraphBullets": {"range": _range(tid, start, start + 1)}},
                {"updateParagraphStyle": {"range": _range(tid, start, start + 1),
                                          "paragraphStyle": {"namedStyleType": "NORMAL_TEXT"}, "fields": "namedStyleType"}},
                {"insertInlineImage": {"location": {"tabId": tid, "index": start}, "uri": image_uri,
                                       "objectSize": stage["new"][0]["size"]}}]
    if kind == "insert_table":
        target = stage["new"][1]
        if phase == "table":
            return [{"insertTable": {"location": {"tabId": tid, "index": nodes[at]["start"]},
                                      "rows": target["rows"], "columns": target["columns"]}}]
        table = nodes[at + 1]
        raw_rows = table["raw"]["table"]["tableRows"]
        requests = []
        for r, row in enumerate(raw_rows):
            for c, cell in enumerate(row["tableCells"]):
                content = cell["content"]
                start = content[0]["startIndex"]
                expected = target["cells"][r][c]
                if phase == "values":
                    text = "".join(p["text"] for p in expected["paragraphs"]).removesuffix("\n")
                    if text:
                        requests.append(_insert(tid, start, text))
                else:
                    requests.extend(_paragraph_requests(tid, start, expected["paragraphs"], insert=False))
                    if expected["style"]:
                        requests.append({"updateTableCellStyle": {"tableRange": {
                            "tableCellLocation": {"tableStartLocation": {"tabId": tid, "index": table["start"]},
                                                  "rowIndex": r, "columnIndex": c}, "rowSpan": 1, "columnSpan": 1},
                            "tableCellStyle": expected["style"], "fields": ",".join(expected["style"])}})
        if phase == "values":
            return sorted(requests, key=lambda req: req["insertText"]["location"]["index"], reverse=True)
        # The paragraph inserted by insertTable can inherit bullets/headings.
        requests.extend(_paragraph_requests(tid, nodes[at]["start"], [stage["new"][0]], insert=False))
        for c, w in enumerate(target.get("widths") or []):
            requests.append({"updateTableColumnProperties": {"tableStartLocation": {"tabId": tid, "index": table["start"]},
                             "columnIndices": [c], "tableColumnProperties": {"widthType": "FIXED_WIDTH", "width": _dimension(w)},
                             "fields": "widthType,width"}})
        return requests
    raise DocsEditError("Unsupported compiled operation.")


def verify_mutation_plan(client, plan, *, receipt=None):
    p = DocsMutationPlan.from_dict(plan.to_dict()).to_dict()
    receipt = receipt or {}
    if receipt and (receipt.get("plan_digest") != p["digest"] or receipt.get("schema_version") != RECEIPT_SCHEMA):
        raise DocsEditError("Receipt does not belong to this plan.")
    document = _read_document(client, p["document_id"])
    mismatch = _check(document, p, p["after"], receipt.get("objects", {}))
    cleanup_ok = receipt.get("cleanup_status", "complete") == "complete"
    return {"schema_version": SCHEMA, "verified": not mismatch,
            "write_status": receipt.get("write_status", "not_observed"), "revision_id": document.get("revisionId"),
            "operations": [{"operation_id": s["id"], "verified": not mismatch} for s in p["stages"]],
            "mismatches": mismatch, "cleanup_status": receipt.get("cleanup_status", "not_observed"),
            "next_step": "done" if not mismatch and cleanup_ok else "Inspect; do not automatically reapply.",
            "ok": not mismatch and cleanup_ok}


def apply_mutation_plan(client, plan, *, apply=False, approved_digest=None, receipt_path=None, image_stager=None):
    """Sequential guarded batches, no retries/rollback. Always journal before writes."""
    p = DocsMutationPlan.from_dict(plan.to_dict()).to_dict()
    if not apply:
        return plan.preview()
    if approved_digest != p["digest"] or receipt_path is None:
        raise DocsEditError("Apply requires the approved digest and a new private receipt path.")
    from .docs_mutation_io import create_receipt, save_receipt
    fresh = _read_document(client, p["document_id"])
    rebuilt = plan_mutations(fresh, operations=p["operations"], tab_id=p["tab_id"])
    if rebuilt.to_dict() != p:
        raise DocsEditError("Document, image or plan changed; plan and review again.")
    if p["assets"] and image_stager is None:
        raise DocsEditError("Images require an explicitly authorized temporary image stager.")
    receipt = {"schema_version": RECEIPT_SCHEMA, "plan_digest": p["digest"], "document_id": p["document_id"],
               "tab_id": p["tab_id"], "revision_id": p["revision_id"], "write_status": "not_started",
               "objects": {}, "stages": [], "assets": [], "ok": False, "verified": False}
    create_receipt(receipt_path, receipt)
    assets, current = {}, fresh
    try:
        for asset in p["assets"]:
            assets[asset["operation_id"]] = image_stager.stage(asset, receipt, receipt_path)
        # Uploads are external writes too, but must not weaken the Docs guard.
        current = _read_document(client, p["document_id"])
        if current.get("revisionId") != p["revision_id"]:
            raise DocsEditError("Revision changed during image staging.")
        for stage in p["stages"]:
            if _check(current, p, stage["before"], receipt["objects"]):
                raise DocsEditError("Intermediate document differs from the reviewed plan.")
            for phase in stage["phases"]:
                requests = _compile(stage, current, p["tab_id"], phase, assets.get(stage["id"]))
                if not requests:
                    continue
                entry = {"operation_id": stage["id"], "phase": phase, "write_status": "unknown"}
                receipt["stages"].append(entry)
                receipt["write_status"] = "unknown"
                save_receipt(receipt_path, receipt)
                response = client._service.documents().batchUpdate(documentId=p["document_id"], body={
                    "requests": requests, "writeControl": {"requiredRevisionId": current["revisionId"]},
                }).execute(num_retries=0)
                entry["write_status"] = "acknowledged"
                receipt["write_status"] = "partial"
                revision = response.get("writeControl", {}).get("requiredRevisionId")
                receipt["revision_id"] = revision
                for reply in response.get("replies", []):
                    if reply.get("insertInlineImage", {}).get("objectId"):
                        receipt["objects"][stage["id"]] = reply["insertInlineImage"]["objectId"]
                save_receipt(receipt_path, receipt)
                current = _read_document(client, p["document_id"])
                if not revision or current.get("revisionId") != revision:
                    raise DocsEditError("Missing or changed post-write revision; inspect before continuing.")
                if phase != stage["phases"][-1]:
                    _check_table_intermediate(current, p, stage, phase, receipt["objects"])
            mismatch = _check(current, p, stage["after"], receipt["objects"])
            if mismatch:
                receipt["mismatches"] = mismatch
                raise DocsEditError("Readback mismatch; inspect without replaying.")
            receipt.setdefault("operations", []).append({"operation_id": stage["id"], "verified": True})
            save_receipt(receipt_path, receipt)
        receipt.update(write_status="acknowledged", verified=True, ok=True, next_step="done")
    except Exception as exc:
        status = getattr(getattr(exc, "resp", None), "status", None)
        if receipt["stages"] and receipt["stages"][-1]["write_status"] == "unknown" and status in {400, 401, 403, 404, 409, 412, 429}:
            receipt["stages"][-1]["write_status"] = "rejected"
            receipt["write_status"] = "partial" if any(s["write_status"] == "acknowledged" for s in receipt["stages"]) else "rejected"
        receipt.update(ok=False, verified=False, error="mutation_stopped",
                       next_step="Inspect receipt and document; verify, never automatically replay.")
        if isinstance(exc, DocsEditError):
            receipt["reason"] = str(exc)
        # Never include API exception strings, request URIs or document bodies.
    finally:
        if image_stager is not None:
            try:
                image_stager.cleanup(receipt, receipt_path)
            except Exception:
                receipt.update(ok=False, cleanup_status="unknown", next_step="Inspect temporary image resources; do not reapply.")
        save_receipt(receipt_path, receipt)
    return receipt


def _check_table_intermediate(document, plan, stage, phase, objects):
    nodes = _nodes(_select(document, plan["tab_id"]))
    expected = deepcopy(stage["after"])
    at = stage["at"]
    if len(nodes) != len(expected) or nodes[at]["kind"] != "paragraph" or nodes[at]["text"] != "\n":
        raise DocsEditError("Unexpected table insertion structure.")
    # Styles are deliberately applied only after actual cell indices are read.
    expected[at] = _expected([nodes[at]])[0]
    target = expected[at + 1]
    actual = nodes[at + 1]
    if actual["kind"] != "table" or (actual["rows"], actual["columns"]) != (target["rows"], target["columns"]):
        raise DocsEditError("Unexpected table dimensions.")
    for er, ar in zip(target["cells"], actual["cells"]):
        for ec, ac in zip(er, ar):
            wanted = "\n" if phase == "table" else "".join(p["text"] for p in ec["paragraphs"])
            if "".join(p["text"] for p in ac["paragraphs"]) != wanted:
                raise DocsEditError("Unexpected table cell contents.")
    expected[at + 1] = _expected([actual])[0]
    if _check(document, plan, expected, objects):
        raise DocsEditError("Outside table content changed.")

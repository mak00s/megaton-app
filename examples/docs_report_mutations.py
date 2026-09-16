"""Build a report plan from consumer-owned headings/text/table data.

No writes, image publication, browser or connector use. Existing images are
located after unique headings; the existing analysis remains after new content.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from megaton_lib.docs_client import DocsClient
from megaton_lib.docs_mutation_io import create_receipt
from megaton_lib.docs_mutations import (
    find_inline_object_after_heading, insert_image_after_anchor,
    insert_paragraphs_after_anchor, insert_table_after_anchor,
    plan_mutations,
)


def report_operations(document, config, *, tab_id):
    ops = []
    for i, comment in enumerate(config["image_comments"]):
        image = find_inline_object_after_heading(document, comment["heading"], tab_id=tab_id)
        ops.append(insert_paragraphs_after_anchor(image, [
            {"text": comment["title"], "paragraph_style": {"namedStyleType": "HEADING_3"}},
            *[{"text": s, "bullet": True} for s in comment["bullets"]]], operation_id=f"comment-{i}"))
    chart = config["chart"]
    ops.append(insert_image_after_anchor(config["news_heading"], chart["path"], width_pt=chart["width_pt"],
                                         height_pt=chart["height_pt"], operation_id="news-chart"))
    ops.append(insert_table_after_anchor({"operation_id": "news-chart"}, config["table_values"],
                                         operation_id="news-table", header_rows=[0],
                                         total_rows=[len(config["table_values"]) - 1],
                                         column_widths_pt=config.get("column_widths_pt")))
    return ops


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--token", required=True)
    p.add_argument("--expected-email", required=True)
    p.add_argument("--document-id", required=True)
    p.add_argument("--tab-id", required=True)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    client = DocsClient.from_oauth_file(args.token, expected_email=args.expected_email)
    document = client.get(args.document_id)
    plan = plan_mutations(document, tab_id=args.tab_id,
                          operations=report_operations(document, json.loads(args.config.read_text()), tab_id=args.tab_id))
    create_receipt(args.output, plan.preview())
    print(json.dumps({"ok": True, "digest": plan.digest, "write_status": "not_started"}))


if __name__ == "__main__":
    main()

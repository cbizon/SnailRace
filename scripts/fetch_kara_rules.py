"""
Fetch KARA typed rules from the aragorn repository and generate packaged JSONL
query files, one file per top-level predicate group.

Source:
  https://github.com/ranking-agent/aragorn/blob/main/src/rules/kara_typed_rules/rules_with_types_cleaned_finalized.json

Usage:
  uv run python scripts/fetch_kara_rules.py
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path

SOURCE_URL = (
    "https://raw.githubusercontent.com/ranking-agent/aragorn/main"
    "/src/rules/kara_typed_rules/rules_with_types_cleaned_finalized.json"
)
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "trapi_performance_tester" / "package_queries"


def key_to_slug(key_str: str) -> str:
    key = json.loads(key_str)
    predicate = key["predicate"].replace("biolink:", "")
    parts = [predicate]
    for qc in key.get("qualifier_constraints", []):
        for q in qc.get("qualifier_set", []):
            if q["qualifier_type_id"] in (
                "biolink:object_aspect_qualifier",
                "biolink:object_direction_qualifier",
            ):
                parts.append(q["qualifier_value"])
    return "_".join(parts)


def strip_target_ids(query_graph: dict) -> dict:
    """Remove ids from nodes whose only id is $target_id (unbound target)."""
    for node in query_graph.get("nodes", {}).values():
        if node.get("ids") == ["$target_id"]:
            del node["ids"]
    return query_graph


def main() -> None:
    print(f"Fetching {SOURCE_URL} ...")
    with urllib.request.urlopen(SOURCE_URL) as response:
        data = json.loads(response.read().decode("utf-8"))

    for key_str, rules in data.items():
        slug = key_to_slug(key_str)
        filename = OUTPUT_DIR / f"kara_{slug}.jsonl"
        lines = []
        for i, rule in enumerate(rules):
            if "template" not in rule:
                continue
            template = json.loads(json.dumps(rule["template"]))
            strip_target_ids(template["query_graph"])
            doc = {
                "query_name": f"{slug}_rule_{i:02d}",
                "message": template,
                "workflow": [{"id": "lookup"}],
            }
            lines.append(json.dumps(doc, separators=(",", ":")))
        filename.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"  Wrote {filename.name} ({len(lines)} queries)")


if __name__ == "__main__":
    main()

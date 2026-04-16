from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


PACKAGE_QUERY_DIR = Path(__file__).resolve().parent / "package_queries"


def default_query_paths() -> list[Path]:
    return sorted(PACKAGE_QUERY_DIR.glob("*.jsonl"))


def load_queries(
    paths: Iterable[str | Path],
    include_names: set[str] | None = None,
) -> list[dict[str, Any]]:
    queries: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(path)

        with path.open("r", encoding="utf-8") as handle:
            for line_number, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    document = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"Invalid JSON in {path}:{line_number}") from error

                if not isinstance(document, dict):
                    raise ValueError(f"Expected a JSON object in {path}:{line_number}")

                query_name = document.get("query_name")
                if not isinstance(query_name, str) or not query_name:
                    raise ValueError(f"Missing query_name in {path}:{line_number}")

                if include_names and query_name not in include_names:
                    continue

                if query_name in seen_names:
                    raise ValueError(f"Duplicate query_name {query_name!r}")

                request_body = dict(document)
                request_body.pop("query_name")

                queries.append(
                    {
                        "query_name": query_name,
                        "query_file": str(path),
                        "request_body": request_body,
                        "metadata": extract_query_metadata(document),
                    }
                )
                seen_names.add(query_name)

    return queries


def extract_query_metadata(document: dict[str, Any]) -> dict[str, Any]:
    query_graph = document.get("message", {}).get("query_graph", {})
    nodes = query_graph.get("nodes", {})
    edges = query_graph.get("edges", {})

    pinned_nodes: list[dict[str, Any]] = []
    pinned_node_ids: list[str] = []
    node_categories: dict[str, list[str]] = {}

    if isinstance(nodes, dict):
        for node_id in sorted(nodes):
            node = nodes[node_id]
            if not isinstance(node, dict):
                continue

            categories = normalize_string_list(node.get("categories"))
            node_categories[node_id] = categories

            ids = normalize_string_list(node.get("ids"))
            if ids:
                pinned_nodes.append(
                    {
                        "qnode_id": node_id,
                        "categories": categories,
                        "ids": ids,
                    }
                )
                pinned_node_ids.extend(ids)

    edge_predicates: list[str] = []
    edge_pairs: list[dict[str, str | list[str]]] = []
    if isinstance(edges, dict):
        for edge_id in sorted(edges):
            edge = edges[edge_id]
            if not isinstance(edge, dict):
                continue

            predicates = normalize_string_list(edge.get("predicates"))
            edge_predicates.extend(predicates)
            edge_pairs.append(
                {
                    "qedge_id": edge_id,
                    "subject": str(edge.get("subject", "")),
                    "object": str(edge.get("object", "")),
                    "predicates": predicates,
                }
            )

    unique_predicates = sorted(set(edge_predicates))

    return {
        "hop_count": len(edge_pairs),
        "pinned_nodes": pinned_nodes,
        "pinned_node_ids": sorted(set(pinned_node_ids)),
        "edge_predicates": unique_predicates,
        "node_categories": node_categories,
        "edge_pairs": edge_pairs,
    }


def normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str)]
    raise ValueError(f"Expected a string or list of strings, got {type(value).__name__}")

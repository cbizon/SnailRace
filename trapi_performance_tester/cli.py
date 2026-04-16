from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .query_io import default_query_paths, load_queries
from .runner import build_query_url, run_benchmark


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    query_paths = args.query_file or [str(path) for path in default_query_paths()]
    include_names = set(args.query_name) if args.query_name else None
    queries = load_queries(query_paths, include_names=include_names, source_id=args.source_id)

    if args.list_queries:
        for query in queries:
            metadata = query["metadata"]
            pinned_ids = ",".join(metadata["pinned_node_ids"]) or "-"
            predicates = ",".join(metadata["edge_predicates"]) or "-"
            print(
                f"{query['query_name']}\t"
                f"hops={metadata['hop_count']}\t"
                f"pinned={pinned_ids}\t"
                f"predicates={predicates}\t"
                f"file={query['query_file']}"
            )
        return 0

    if not args.endpoint:
        parser.error("at least one --endpoint is required unless --list-queries is used")

    endpoints = [parse_endpoint(value) for value in args.endpoint]
    save_response_dir = Path(args.save_responses).resolve() if args.save_responses else None

    report = run_benchmark(
        endpoints=endpoints,
        queries=queries,
        iterations=args.iterations,
        timeout_seconds=args.timeout_seconds,
        save_response_dir=save_response_dir,
        progress=lambda message: print(message, file=sys.stderr),
    )

    output_path = args.output or default_output_path()
    write_report(report, output_path)
    if output_path != "-":
        print(f"Wrote {output_path}", file=sys.stderr)
    print_run_summary(report)
    return 1 if report["has_failures"] else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run JSONL TRAPI queries against one or more TRAPI endpoints.",
    )
    parser.add_argument(
        "--endpoint",
        action="append",
        required=False,
        default=[],
        help="Endpoint as name=url or bare URL. Repeat for multiple endpoints.",
    )
    parser.add_argument(
        "--query-file",
        action="append",
        help="JSONL query file. Defaults to the packaged query sets.",
    )
    parser.add_argument(
        "--query-name",
        action="append",
        help="Limit the run to specific query_name values.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of times to run each query against each endpoint.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=300.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--output",
        help="JSON output path. Use '-' to write to stdout.",
    )
    parser.add_argument(
        "--save-responses",
        help="Directory for raw TRAPI response bodies.",
    )
    parser.add_argument(
        "--source-id",
        help="CURIE to substitute for $source_id in template queries (e.g. CHEBI:45783).",
    )
    parser.add_argument(
        "--list-queries",
        action="store_true",
        help="Print the loaded query catalog and exit.",
    )
    return parser


def parse_endpoint(value: str) -> dict[str, str]:
    if "=" in value:
        name, raw_url = value.split("=", 1)
    else:
        raw_url = value
        parsed = urlsplit(value)
        name = parsed.netloc or parsed.path or value

    name = name.strip()
    base_url = raw_url.strip()
    if not name:
        raise ValueError(f"Invalid endpoint specification {value!r}")
    if not base_url:
        raise ValueError(f"Invalid endpoint specification {value!r}")

    return {
        "name": name,
        "base_url": base_url,
        "query_url": build_query_url(base_url),
    }


def default_output_path() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return str(Path("results") / f"trapi_performance_{timestamp}.json")


def print_run_summary(report: dict[str, Any]) -> None:
    records = report["records"]
    total_time = sum(r["elapsed_seconds"] for r in records)
    total_results = sum(r["result_count"] for r in records if r["result_count"] is not None)
    failures = report["summaries"]["overall"]["failure_count"]
    parts = [
        f"requests={report['request_count']}",
        f"failures={failures}",
        f"results={total_results}",
        f"time={total_time:.1f}s",
    ]
    print("  ".join(parts), file=sys.stderr)


def write_report(report: dict[str, Any], output_path: str) -> None:
    payload = json.dumps(report, indent=2)
    if output_path == "-":
        print(payload)
        return

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{payload}\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

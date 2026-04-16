from __future__ import annotations

import json
import math
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def build_query_url(base_url: str) -> str:
    stripped = base_url.rstrip("/?")
    if stripped.endswith("/query"):
        return stripped
    return f"{stripped}/query"


def run_benchmark(
    endpoints: list[dict[str, str]],
    queries: list[dict[str, Any]],
    iterations: int,
    timeout_seconds: float,
    save_response_dir: Path | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be at least 1")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    started_at = datetime.now(timezone.utc)
    records: list[dict[str, Any]] = []
    total_requests = len(endpoints) * len(queries) * iterations
    request_number = 0

    for endpoint in endpoints:
        for query in queries:
            for iteration in range(1, iterations + 1):
                request_number += 1
                if progress is not None:
                    progress(
                        f"[{request_number}/{total_requests}] "
                        f"{endpoint['name']} {query['query_name']} iter {iteration}"
                    )

                records.append(
                    execute_query(
                        endpoint=endpoint,
                        query=query,
                        iteration=iteration,
                        timeout_seconds=timeout_seconds,
                        save_response_dir=save_response_dir,
                    )
                )

    finished_at = datetime.now(timezone.utc)
    return {
        "started_at": isoformat_utc(started_at),
        "finished_at": isoformat_utc(finished_at),
        "elapsed_seconds": (finished_at - started_at).total_seconds(),
        "iterations": iterations,
        "endpoint_count": len(endpoints),
        "query_count": len(queries),
        "request_count": len(records),
        "has_failures": any(record["error"] for record in records),
        "endpoints": endpoints,
        "query_catalog": build_query_catalog(queries),
        "records": records,
        "summaries": summarize_records(records),
    }


def execute_query(
    endpoint: dict[str, str],
    query: dict[str, Any],
    iteration: int,
    timeout_seconds: float,
    save_response_dir: Path | None = None,
) -> dict[str, Any]:
    query_url = endpoint["query_url"]
    request_bytes = json.dumps(
        query["request_body"],
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    request = urllib.request.Request(
        query_url,
        data=request_bytes,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    started_at = datetime.now(timezone.utc)
    start = time.perf_counter()

    status_code: int | None = None
    response_bytes = b""
    response_json: dict[str, Any] | None = None
    error_text: str | None = None

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.status
            response_bytes = response.read()
    except urllib.error.HTTPError as error:
        status_code = error.code
        response_bytes = error.read()
        error_text = f"HTTP {error.code}: {error.reason}"
    except urllib.error.URLError as error:
        error_text = f"{type(error.reason).__name__}: {error.reason}"
    except TimeoutError as error:
        error_text = f"{type(error).__name__}: {error}"

    elapsed_seconds = time.perf_counter() - start

    if response_bytes:
        try:
            decoded = response_bytes.decode("utf-8")
            loaded = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            error_text = combine_errors(error_text, f"Response was not valid JSON: {error}")
        else:
            if isinstance(loaded, dict):
                response_json = loaded
            else:
                error_text = combine_errors(error_text, "Response JSON was not an object")

    metrics = extract_trapi_metrics(response_json)
    error_text = combine_errors(error_text, metrics["error"])
    saved_response_path = save_response(
        save_response_dir,
        endpoint_name=endpoint["name"],
        query_name=query["query_name"],
        iteration=iteration,
        response_bytes=response_bytes,
    )

    record = {
        "endpoint_name": endpoint["name"],
        "endpoint_url": endpoint["base_url"],
        "query_url": query_url,
        "query_name": query["query_name"],
        "query_file": query["query_file"],
        "iteration": iteration,
        "started_at": isoformat_utc(started_at),
        "elapsed_seconds": elapsed_seconds,
        "status_code": status_code,
        "request_size_bytes": len(request_bytes),
        "response_size_bytes": len(response_bytes),
        "result_count": metrics["result_count"],
        "kg_node_count": metrics["kg_node_count"],
        "kg_edge_count": metrics["kg_edge_count"],
        "saved_response_path": str(saved_response_path) if saved_response_path else None,
        "error": error_text,
    }
    record.update(query["metadata"])
    return record


def extract_trapi_metrics(response_json: dict[str, Any] | None) -> dict[str, Any]:
    if response_json is None:
        return {
            "result_count": None,
            "kg_node_count": None,
            "kg_edge_count": None,
            "error": None,
        }

    message = response_json.get("message")
    if not isinstance(message, dict):
        description = response_json.get("description")
        if isinstance(description, str) and description:
            error = description
        else:
            error = "TRAPI response missing message object"
        return {
            "result_count": None,
            "kg_node_count": None,
            "kg_edge_count": None,
            "error": error,
        }

    results = message.get("results")
    if not isinstance(results, list):
        return {
            "result_count": None,
            "kg_node_count": None,
            "kg_edge_count": None,
            "error": "TRAPI response missing message.results list",
        }

    knowledge_graph = message.get("knowledge_graph", {})
    if knowledge_graph is None:
        knowledge_graph = {}
    if not isinstance(knowledge_graph, dict):
        return {
            "result_count": len(results),
            "kg_node_count": None,
            "kg_edge_count": None,
            "error": "TRAPI response knowledge_graph was not an object",
        }

    nodes = knowledge_graph.get("nodes", {})
    edges = knowledge_graph.get("edges", {})
    if not isinstance(nodes, dict) or not isinstance(edges, dict):
        return {
            "result_count": len(results),
            "kg_node_count": None,
            "kg_edge_count": None,
            "error": "TRAPI response knowledge_graph nodes or edges were not objects",
        }

    return {
        "result_count": len(results),
        "kg_node_count": len(nodes),
        "kg_edge_count": len(edges),
        "error": None,
    }


def build_query_catalog(queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for query in queries:
        entry = {
            "query_name": query["query_name"],
            "query_file": query["query_file"],
        }
        entry.update(query["metadata"])
        catalog.append(entry)
    return catalog


def summarize_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "overall": summarize_group(records),
        "by_endpoint": summarize_mapping(
            records,
            key_builder=lambda record: record["endpoint_name"],
            metadata_builder=lambda first: {
                "endpoint_name": first["endpoint_name"],
                "endpoint_url": first["endpoint_url"],
            },
        ),
        "by_query": summarize_mapping(
            records,
            key_builder=lambda record: record["query_name"],
            metadata_builder=query_summary_metadata,
        ),
        "by_endpoint_query": summarize_mapping(
            records,
            key_builder=lambda record: f"{record['endpoint_name']}::{record['query_name']}",
            metadata_builder=lambda first: {
                "endpoint_name": first["endpoint_name"],
                "endpoint_url": first["endpoint_url"],
                **query_summary_metadata(first),
            },
        ),
        "by_hop_count": summarize_mapping(
            records,
            key_builder=lambda record: str(record["hop_count"]),
            metadata_builder=lambda first: {"hop_count": first["hop_count"]},
        ),
        "by_pinned_node_id": summarize_pinned_nodes(records),
    }


def summarize_mapping(
    records: list[dict[str, Any]],
    key_builder: Callable[[dict[str, Any]], str],
    metadata_builder: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[key_builder(record)].append(record)

    summaries: dict[str, Any] = {}
    for key in sorted(groups):
        grouped_records = groups[key]
        summary = summarize_group(grouped_records)
        summary.update(metadata_builder(grouped_records[0]))
        summaries[key] = summary
    return summaries


def summarize_pinned_nodes(records: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        for pinned_id in record["pinned_node_ids"]:
            groups[pinned_id].append(record)

    summaries: dict[str, Any] = {}
    for pinned_id in sorted(groups):
        summary = summarize_group(groups[pinned_id])
        summary["pinned_node_id"] = pinned_id
        summaries[pinned_id] = summary
    return summaries


def summarize_group(records: list[dict[str, Any]]) -> dict[str, Any]:
    successful_records = [record for record in records if not record["error"]]

    return {
        "request_count": len(records),
        "success_count": len(successful_records),
        "failure_count": len(records) - len(successful_records),
        "status_codes": dict(
            sorted(
                Counter(str(record["status_code"]) for record in records).items(),
                key=lambda item: item[0],
            )
        ),
        "elapsed_seconds": summarize_numeric(record["elapsed_seconds"] for record in records),
        "successful_elapsed_seconds": summarize_numeric(
            record["elapsed_seconds"] for record in successful_records
        ),
        "request_size_bytes": summarize_numeric(record["request_size_bytes"] for record in records),
        "response_size_bytes": summarize_numeric(
            record["response_size_bytes"] for record in successful_records
        ),
        "result_count": summarize_numeric(
            record["result_count"]
            for record in successful_records
            if record["result_count"] is not None
        ),
        "kg_node_count": summarize_numeric(
            record["kg_node_count"]
            for record in successful_records
            if record["kg_node_count"] is not None
        ),
        "kg_edge_count": summarize_numeric(
            record["kg_edge_count"]
            for record in successful_records
            if record["kg_edge_count"] is not None
        ),
    }


def summarize_numeric(values: Any) -> dict[str, float] | None:
    numeric_values = list(values)
    if not numeric_values:
        return None

    sorted_values = sorted(numeric_values)
    return {
        "count": len(sorted_values),
        "min": sorted_values[0],
        "max": sorted_values[-1],
        "mean": statistics.fmean(sorted_values),
        "median": statistics.median(sorted_values),
        "p95": percentile(sorted_values, 0.95),
    }


def percentile(sorted_values: list[float], value: float) -> float:
    if not sorted_values:
        raise ValueError("sorted_values must not be empty")
    index = max(math.ceil(len(sorted_values) * value) - 1, 0)
    return sorted_values[index]


def query_summary_metadata(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "query_name": record["query_name"],
        "query_file": record["query_file"],
        "hop_count": record["hop_count"],
        "pinned_node_ids": record["pinned_node_ids"],
        "edge_predicates": record["edge_predicates"],
    }


def save_response(
    save_response_dir: Path | None,
    endpoint_name: str,
    query_name: str,
    iteration: int,
    response_bytes: bytes,
) -> Path | None:
    if save_response_dir is None or not response_bytes:
        return None

    endpoint_dir = save_response_dir / sanitize_path_part(endpoint_name)
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    response_path = endpoint_dir / (
        f"{sanitize_path_part(query_name)}_iter{iteration}.json"
    )
    response_path.write_bytes(response_bytes)
    return response_path


def sanitize_path_part(value: str) -> str:
    sanitized = []
    for character in value:
        if character.isalnum() or character in ("-", "_", "."):
            sanitized.append(character)
        else:
            sanitized.append("_")
    return "".join(sanitized)


def combine_errors(existing: str | None, new: str | None) -> str | None:
    if not existing:
        return new
    if not new:
        return existing
    return f"{existing}; {new}"


def isoformat_utc(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")

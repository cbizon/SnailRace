from __future__ import annotations

import json
import math
import statistics
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit
from uuid import uuid4


def build_query_url(base_url: str) -> str:
    stripped = base_url.rstrip("/?")
    if stripped.endswith("/query") or stripped.endswith("/asyncquery"):
        return stripped
    return f"{stripped}/query"


def run_benchmark(
    endpoints: list[dict[str, Any]],
    queries: list[dict[str, Any]],
    iterations: int,
    timeout_seconds: float,
    save_response_dir: Path | None = None,
    progress: Callable[[str], None] | None = None,
    log_requests: Callable[[str], None] | None = None,
    callback_bind_host: str = "127.0.0.1",
    callback_port: int = 0,
    callback_base_url: str | None = None,
) -> dict[str, Any]:
    if iterations < 1:
        raise ValueError("iterations must be at least 1")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    normalized_endpoints = [normalize_endpoint(endpoint) for endpoint in endpoints]
    started_at = datetime.now(timezone.utc)
    work_items = build_work_items(normalized_endpoints, queries, iterations)
    total_requests = len(work_items)
    async_items = [item for item in work_items if item["endpoint"]["mode"] == "asyncquery"]
    sync_items = [item for item in work_items if item["endpoint"]["mode"] != "asyncquery"]
    records_by_request_number: dict[int, dict[str, Any]] = {}

    callback_manager = (
        open_callback_server(
            bind_host=callback_bind_host,
            port=callback_port,
            base_url=callback_base_url,
        )
        if async_items
        else nullcontext(None)
    )

    with callback_manager as callback_context:
        pending_async: list[dict[str, Any]] = []

        for item in async_items:
            if progress is not None:
                progress(f"{request_progress_label(item, total_requests)} async submit")

            submission = submit_async_query(
                endpoint=item["endpoint"],
                query=item["query"],
                iteration=item["iteration"],
                timeout_seconds=timeout_seconds,
                save_response_dir=save_response_dir,
                callback_context=callback_context,
                log_requests=log_requests,
            )

            if "record" in submission:
                record = submission["record"]
                records_by_request_number[item["request_number"]] = record
                report_record_progress(record, progress)
                continue

            pending_async.append(
                {
                    "request_number": item["request_number"],
                    "endpoint": item["endpoint"],
                    "query": item["query"],
                    "iteration": item["iteration"],
                    **submission,
                }
            )
            if progress is not None:
                progress(f"  submitted callback {submission['callback_url']}")

        for item in sync_items:
            if progress is not None:
                progress(request_progress_label(item, total_requests))

            record = execute_query(
                endpoint=item["endpoint"],
                query=item["query"],
                iteration=item["iteration"],
                timeout_seconds=timeout_seconds,
                save_response_dir=save_response_dir,
                log_requests=log_requests,
            )
            records_by_request_number[item["request_number"]] = record
            report_record_progress(record, progress)

        async_records = collect_async_records(
            pending_async=pending_async,
            callback_context=callback_context,
            save_response_dir=save_response_dir,
        )
        for request_number, record in async_records.items():
            records_by_request_number[request_number] = record
            report_record_progress(record, progress)

    finished_at = datetime.now(timezone.utc)
    records = [
        records_by_request_number[request_number]
        for request_number in range(1, total_requests + 1)
    ]
    return {
        "started_at": isoformat_utc(started_at),
        "finished_at": isoformat_utc(finished_at),
        "elapsed_seconds": (finished_at - started_at).total_seconds(),
        "iterations": iterations,
        "endpoint_count": len(normalized_endpoints),
        "query_count": len(queries),
        "request_count": len(records),
        "has_failures": any(record["error"] for record in records),
        "endpoints": normalized_endpoints,
        "query_catalog": build_query_catalog(queries),
        "records": records,
        "summaries": summarize_records(records),
    }


def build_work_items(
    endpoints: list[dict[str, Any]],
    queries: list[dict[str, Any]],
    iterations: int,
) -> list[dict[str, Any]]:
    work_items: list[dict[str, Any]] = []
    request_number = 0
    for endpoint in endpoints:
        for query in queries:
            for iteration in range(1, iterations + 1):
                request_number += 1
                work_items.append(
                    {
                        "request_number": request_number,
                        "endpoint": endpoint,
                        "query": query,
                        "iteration": iteration,
                    }
                )
    return work_items


def normalize_endpoint(endpoint: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(endpoint)
    base_url = str(normalized["base_url"])
    normalized["base_url"] = base_url
    normalized["query_url"] = str(normalized.get("query_url") or build_query_url(base_url))
    normalized["mode"] = endpoint_request_mode(normalized)
    return normalized


def endpoint_request_mode(endpoint: dict[str, Any]) -> str:
    explicit_mode = str(endpoint.get("mode", "")).strip().lower()
    if explicit_mode in {"query", "sync"}:
        return "query"
    if explicit_mode in {"async", "asyncquery"}:
        return "asyncquery"

    query_url = str(endpoint.get("query_url", "")).rstrip("/?")
    if query_url.endswith("/asyncquery"):
        return "asyncquery"
    return "query"


def execute_query(
    endpoint: dict[str, Any],
    query: dict[str, Any],
    iteration: int,
    timeout_seconds: float,
    save_response_dir: Path | None = None,
    log_requests: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    query_url = endpoint["query_url"]
    request_body = query["request_body"]
    request_bytes = encode_request_body(request_body)

    if log_requests is not None:
        pretty = json.dumps(request_body, indent=2)
        log_requests(f"POST {query_url}\n{pretty}")

    started_at = datetime.now(timezone.utc)
    start = time.perf_counter()
    status_code, response_bytes, error_text = post_json_request(
        query_url,
        request_bytes,
        timeout_seconds,
    )
    elapsed_seconds = time.perf_counter() - start
    response_json, response_error = parse_response_bytes(response_bytes)
    error_text = combine_errors(error_text, response_error)

    return build_record(
        endpoint=endpoint,
        query=query,
        iteration=iteration,
        started_at=started_at,
        elapsed_seconds=elapsed_seconds,
        status_code=status_code,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        response_json=response_json,
        error_text=error_text,
        save_response_dir=save_response_dir,
    )


def async_submit_was_accepted(
    status_code: int | None,
    submit_payload: dict[str, Any] | None,
    error_text: str | None,
) -> bool:
    if error_text is not None:
        return False
    if status_code == 202:
        return True
    if status_code != 200:
        return False
    if not isinstance(submit_payload, dict):
        return False

    status = submit_payload.get("status")
    callback = submit_payload.get("callback")
    return (isinstance(status, str) and status.lower() == "accepted") or isinstance(
        callback, str
    )


def submit_async_query(
    endpoint: dict[str, Any],
    query: dict[str, Any],
    iteration: int,
    timeout_seconds: float,
    save_response_dir: Path | None,
    callback_context: dict[str, Any],
    log_requests: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    query_url = endpoint["query_url"]
    request_id = uuid4().hex
    callback_url = build_callback_url(callback_context, request_id)
    request_body = dict(query["request_body"])
    request_body["callback"] = callback_url
    request_bytes = encode_request_body(request_body)

    if log_requests is not None:
        pretty = json.dumps(request_body, indent=2)
        log_requests(f"POST {query_url}\n{pretty}")

    started_at = datetime.now(timezone.utc)
    start = time.perf_counter()
    status_code, response_bytes, error_text = post_json_request(
        query_url,
        request_bytes,
        timeout_seconds,
    )
    submit_elapsed_seconds = time.perf_counter() - start
    submit_payload, submit_response_error = parse_response_bytes(response_bytes)
    job_id = submit_payload.get("job_id") if isinstance(submit_payload, dict) else None

    error_text = combine_errors(error_text, submit_response_error)
    if not async_submit_was_accepted(status_code, submit_payload, error_text):
        if status_code is not None:
            error_text = combine_errors(
                error_text,
                f"Unexpected async submit status {status_code}",
            )
        return {
            "record": build_record(
                endpoint=endpoint,
                query=query,
                iteration=iteration,
                started_at=started_at,
                elapsed_seconds=submit_elapsed_seconds,
                status_code=status_code,
                request_bytes=request_bytes,
                response_bytes=response_bytes,
                response_json=submit_payload,
                error_text=error_text,
                save_response_dir=save_response_dir,
                extra_fields={
                    "submit_status_code": status_code,
                    "async_request_id": request_id,
                    "async_callback_url": callback_url,
                    "async_job_id": job_id,
                },
            )
        }

    return {
        "request_id": request_id,
        "callback_url": callback_url,
        "request_bytes": request_bytes,
        "started_at": started_at,
        "started_perf_counter": start,
        "deadline_perf_counter": start + timeout_seconds,
        "timeout_seconds": timeout_seconds,
        "submit_status_code": status_code,
        "async_job_id": job_id,
    }


def collect_async_records(
    pending_async: list[dict[str, Any]],
    callback_context: dict[str, Any],
    save_response_dir: Path | None = None,
) -> dict[int, dict[str, Any]]:
    pending_by_request_id = {
        pending["request_id"]: pending
        for pending in pending_async
    }
    records: dict[int, dict[str, Any]] = {}

    while pending_by_request_id:
        ready: list[tuple[dict[str, Any], dict[str, Any] | None]] = []

        with callback_context["condition"]:
            now = time.perf_counter()
            wait_seconds: float | None = None

            for request_id, pending in list(pending_by_request_id.items()):
                callback_response = callback_context["responses"].get(request_id)
                if callback_response is not None:
                    ready.append((pending, callback_response))
                    pending_by_request_id.pop(request_id)
                    continue

                remaining_seconds = pending["deadline_perf_counter"] - now
                if remaining_seconds <= 0:
                    ready.append((pending, None))
                    pending_by_request_id.pop(request_id)
                    continue

                if wait_seconds is None or remaining_seconds < wait_seconds:
                    wait_seconds = remaining_seconds

            if not ready and pending_by_request_id:
                callback_context["condition"].wait(timeout=wait_seconds)
                continue

        for pending, callback_response in ready:
            records[pending["request_number"]] = finalize_async_record(
                pending=pending,
                callback_response=callback_response,
                save_response_dir=save_response_dir,
            )

    return records


def finalize_async_record(
    pending: dict[str, Any],
    callback_response: dict[str, Any] | None,
    save_response_dir: Path | None = None,
) -> dict[str, Any]:
    error_text: str | None = None
    response_json: dict[str, Any] | None = None
    response_bytes = b""
    status_code = pending["submit_status_code"]
    elapsed_seconds = pending["timeout_seconds"]

    if callback_response is None:
        error_text = (
            "TimeoutError: Timed out after "
            f"{pending['timeout_seconds']:.3f}s waiting for async callback"
        )
    else:
        response_json = callback_response["payload"]
        response_bytes = callback_response["response_bytes"]
        status_code = callback_status_code(response_json, pending["submit_status_code"])
        elapsed_seconds = callback_response["received_perf_counter"] - pending["started_perf_counter"]
        error_text = callback_response["error"]
        if callback_response["received_perf_counter"] > pending["deadline_perf_counter"]:
            timeout_error = (
                "TimeoutError: Timed out after "
                f"{pending['timeout_seconds']:.3f}s waiting for async callback"
            )
            error_text = combine_errors(timeout_error, error_text)

    return build_record(
        endpoint=pending["endpoint"],
        query=pending["query"],
        iteration=pending["iteration"],
        started_at=pending["started_at"],
        elapsed_seconds=elapsed_seconds,
        status_code=status_code,
        request_bytes=pending["request_bytes"],
        response_bytes=response_bytes,
        response_json=response_json,
        error_text=error_text,
        save_response_dir=save_response_dir,
        extra_fields={
            "submit_status_code": pending["submit_status_code"],
            "async_request_id": pending["request_id"],
            "async_callback_url": pending["callback_url"],
            "async_job_id": pending["async_job_id"],
        },
    )


def build_record(
    endpoint: dict[str, Any],
    query: dict[str, Any],
    iteration: int,
    started_at: datetime,
    elapsed_seconds: float,
    status_code: int | None,
    request_bytes: bytes,
    response_bytes: bytes,
    response_json: dict[str, Any] | None,
    error_text: str | None,
    save_response_dir: Path | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
        "query_url": endpoint["query_url"],
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
        "endpoint_mode": endpoint["mode"],
    }
    if extra_fields:
        record.update(extra_fields)
    record.update(query["metadata"])
    return record


def post_json_request(
    request_url: str,
    request_bytes: bytes,
    timeout_seconds: float,
) -> tuple[int | None, bytes, str | None]:
    request = urllib.request.Request(
        request_url,
        data=request_bytes,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return response.status, response.read(), None
    except urllib.error.HTTPError as error:
        return error.code, error.read(), f"HTTP {error.code}: {error.reason}"
    except urllib.error.URLError as error:
        return None, b"", f"{type(error.reason).__name__}: {error.reason}"
    except TimeoutError as error:
        return None, b"", f"{type(error).__name__}: {error}"


def encode_request_body(request_body: dict[str, Any]) -> bytes:
    return json.dumps(
        request_body,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def parse_response_bytes(
    response_bytes: bytes,
    context_label: str = "Response",
) -> tuple[dict[str, Any] | None, str | None]:
    if not response_bytes:
        return None, None

    try:
        decoded = response_bytes.decode("utf-8")
        loaded = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return None, f"{context_label} was not valid JSON: {error}"

    if not isinstance(loaded, dict):
        return None, f"{context_label} JSON was not an object"
    return loaded, None


def callback_status_code(response_json: dict[str, Any] | None, fallback: int | None) -> int | None:
    if isinstance(response_json, dict):
        http_code = response_json.get("http_code")
        if isinstance(http_code, int):
            return http_code
    return fallback


@contextmanager
def open_callback_server(
    bind_host: str,
    port: int,
    base_url: str | None = None,
):
    if base_url is None and bind_host in {"", "0.0.0.0", "::"}:
        raise ValueError(
            "callback_base_url is required when callback_bind_host is a wildcard address"
        )

    callback_root, callback_path_prefix = normalize_callback_root(base_url)
    bind_port = resolve_callback_bind_port(port, callback_root)
    callback_state: dict[str, Any] = {
        "condition": threading.Condition(),
        "responses": {},
        "callback_root": callback_root,
        "callback_path_prefix": callback_path_prefix,
    }

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            request_id = extract_callback_request_id(
                self.path,
                callback_state["callback_path_prefix"],
            )
            if request_id is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            content_length = int(self.headers.get("Content-Length", "0"))
            response_bytes = self.rfile.read(content_length)
            response_json, error_text = parse_response_bytes(
                response_bytes,
                context_label="Callback payload",
            )

            with callback_state["condition"]:
                callback_state["responses"][request_id] = {
                    "payload": response_json,
                    "response_bytes": response_bytes,
                    "error": error_text,
                    "received_perf_counter": time.perf_counter(),
                }
                callback_state["condition"].notify_all()

            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format, *args) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer((bind_host, bind_port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        if callback_root is None:
            callback_root = f"http://{format_host_for_url(bind_host)}:{server.server_address[1]}"
            callback_state["callback_root"] = callback_root
        yield callback_state
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def normalize_callback_root(base_url: str | None) -> tuple[str | None, str]:
    if base_url is None:
        return None, ""

    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("callback_base_url must be an absolute http:// or https:// URL")

    callback_root = base_url.rstrip("/")
    path_prefix = parsed.path.rstrip("/")
    return callback_root, path_prefix


def resolve_callback_bind_port(port: int, callback_root: str | None) -> int:
    if port:
        return port
    if callback_root is None:
        return 0

    parsed = urlsplit(callback_root)
    if parsed.port is not None:
        return parsed.port
    raise ValueError(
        "callback_port must be set when callback_base_url does not include an explicit port"
    )


def build_callback_url(callback_context: dict[str, Any], request_id: str) -> str:
    return f"{callback_context['callback_root']}/callback/{request_id}"


def extract_callback_request_id(path: str, path_prefix: str) -> str | None:
    callback_path = urlsplit(path).path
    expected_prefix = f"{path_prefix}/callback/"
    if not callback_path.startswith(expected_prefix):
        return None
    request_id = callback_path[len(expected_prefix):]
    if not request_id or "/" in request_id:
        return None
    return request_id


def format_host_for_url(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def request_progress_label(item: dict[str, Any], total_requests: int) -> str:
    endpoint = item["endpoint"]
    query = item["query"]
    return (
        f"[{item['request_number']}/{total_requests}] "
        f"{endpoint['name']} {query['query_name']} iter {item['iteration']}"
    )


def report_record_progress(
    record: dict[str, Any],
    progress: Callable[[str], None] | None = None,
) -> None:
    if progress is None:
        return

    results = record["result_count"]
    result_str = f"{results} results" if results is not None else "no results"
    progress(f"  {record['elapsed_seconds']:.1f}s  {result_str}")
    if record["error"]:
        progress(f"  ERROR: {record['error']}")


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

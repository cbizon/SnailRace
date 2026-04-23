from __future__ import annotations

import json
import threading
import urllib.request
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from trapi_performance_tester.query_io import load_queries
from trapi_performance_tester.runner import build_query_url, run_benchmark


def test_build_query_url_accepts_base_query_or_asyncquery_url() -> None:
    assert build_query_url("https://example.org/trapi") == "https://example.org/trapi/query"
    assert build_query_url("https://example.org/trapi/") == "https://example.org/trapi/query"
    assert build_query_url("https://example.org/trapi/query") == "https://example.org/trapi/query"
    assert build_query_url("https://example.org/trapi/query?") == "https://example.org/trapi/query"
    assert build_query_url("https://example.org/trapi/asyncquery") == "https://example.org/trapi/asyncquery"
    assert build_query_url("https://example.org/trapi/asyncquery?") == "https://example.org/trapi/asyncquery"


def test_run_benchmark_collects_metrics_and_pinned_node_summary(tmp_path) -> None:
    query_document = {
        "query_name": "local_query",
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {
                        "categories": ["biolink:ChemicalEntity"],
                        "ids": ["CHEBI:1"],
                    },
                    "n1": {
                        "categories": ["biolink:Disease"],
                    },
                },
                "edges": {
                    "e01": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:treats"],
                    }
                },
            }
        },
        "workflow": [{"id": "lookup"}],
    }
    query_path = tmp_path / "queries.jsonl"
    query_path.write_text(f"{json.dumps(query_document)}\n", encoding="utf-8")
    queries = load_queries([query_path])

    response_body = {
        "message": {
            "knowledge_graph": {
                "nodes": {"a": {}, "b": {}},
                "edges": {"ab": {}},
            },
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "CHEBI:1"}]},
                    "analyses": [{"edge_bindings": {"e01": [{"id": "ab"}]}}],
                }
            ],
        }
    }

    with run_test_server(response_body) as server_info:
        base_url, requests = server_info
        report = run_benchmark(
            endpoints=[
                {
                    "name": "local",
                    "base_url": base_url,
                    "query_url": build_query_url(base_url),
                }
            ],
            queries=queries,
            iterations=2,
            timeout_seconds=5.0,
            save_response_dir=tmp_path / "responses",
        )

    assert report["request_count"] == 2
    assert report["has_failures"] is False
    assert len(requests) == 2
    assert requests[0]["path"] == "/query"
    assert "query_name" not in requests[0]["payload"]
    assert requests[0]["payload"]["message"]["query_graph"]["nodes"]["n0"]["ids"] == ["CHEBI:1"]

    first_record = report["records"][0]
    assert first_record["endpoint_name"] == "local"
    assert first_record["query_name"] == "local_query"
    assert first_record["result_count"] == 1
    assert first_record["kg_node_count"] == 2
    assert first_record["kg_edge_count"] == 1
    assert first_record["error"] is None
    assert first_record["saved_response_path"] is not None

    endpoint_summary = report["summaries"]["by_endpoint"]["local"]
    assert endpoint_summary["request_count"] == 2
    assert endpoint_summary["success_count"] == 2
    assert endpoint_summary["result_count"]["mean"] == 1.0

    query_summary = report["summaries"]["by_query"]["local_query"]
    assert query_summary["hop_count"] == 1
    assert query_summary["pinned_node_ids"] == ["CHEBI:1"]
    assert query_summary["elapsed_seconds"]["count"] == 2

    pinned_summary = report["summaries"]["by_pinned_node_id"]["CHEBI:1"]
    assert pinned_summary["request_count"] == 2
    assert pinned_summary["success_count"] == 2


@pytest.mark.parametrize(
    ("submit_status_code", "submit_status_field"),
    [
        (202, "Accepted"),
        (200, "accepted"),
    ],
)
def test_run_benchmark_submits_async_queries_before_waiting_for_callbacks(
    tmp_path,
    submit_status_code: int,
    submit_status_field: str,
) -> None:
    query_documents = [
        {
            "query_name": "async_query_one",
            "message": {
                "query_graph": {
                    "nodes": {
                        "n0": {
                            "categories": ["biolink:ChemicalEntity"],
                            "ids": ["CHEBI:1"],
                        },
                        "n1": {
                            "categories": ["biolink:Disease"],
                        },
                    },
                    "edges": {
                        "e01": {
                            "subject": "n0",
                            "object": "n1",
                            "predicates": ["biolink:treats"],
                        }
                    },
                }
            },
            "workflow": [{"id": "lookup"}],
        },
        {
            "query_name": "async_query_two",
            "message": {
                "query_graph": {
                    "nodes": {
                        "n0": {
                            "categories": ["biolink:ChemicalEntity"],
                            "ids": ["CHEBI:2"],
                        },
                        "n1": {
                            "categories": ["biolink:Disease"],
                        },
                    },
                    "edges": {
                        "e01": {
                            "subject": "n0",
                            "object": "n1",
                            "predicates": ["biolink:treats"],
                        }
                    },
                }
            },
            "workflow": [{"id": "lookup"}],
        },
    ]
    query_path = tmp_path / "async_queries.jsonl"
    query_path.write_text(
        "\n".join(json.dumps(document) for document in query_documents) + "\n",
        encoding="utf-8",
    )
    queries = load_queries([query_path])

    callback_payloads = [
        build_async_callback_payload("CHEBI:1"),
        build_async_callback_payload("CHEBI:2"),
    ]

    with run_async_test_server(
        callback_payloads,
        submit_status_code=submit_status_code,
        submit_status_field=submit_status_field,
    ) as server_info:
        base_url, requests = server_info
        async_url = f"{base_url}/asyncquery"
        report = run_benchmark(
            endpoints=[
                {
                    "name": "local-async",
                    "base_url": async_url,
                    "query_url": build_query_url(async_url),
                }
            ],
            queries=queries,
            iterations=1,
            timeout_seconds=5.0,
            save_response_dir=tmp_path / "responses",
        )

    assert report["request_count"] == 2
    assert report["has_failures"] is False
    assert len(requests) == 2
    assert requests[0]["path"] == "/asyncquery"
    assert requests[1]["path"] == "/asyncquery"
    assert requests[0]["payload"]["callback"] != requests[1]["payload"]["callback"]
    assert requests[0]["payload"]["callback"].startswith("http://127.0.0.1:")
    assert requests[1]["payload"]["callback"].startswith("http://127.0.0.1:")

    first_record, second_record = report["records"]
    assert first_record["query_name"] == "async_query_one"
    assert second_record["query_name"] == "async_query_two"
    assert first_record["query_url"] == async_url
    assert first_record["endpoint_mode"] == "asyncquery"
    assert first_record["submit_status_code"] == submit_status_code
    assert first_record["status_code"] == 200
    assert first_record["result_count"] == 1
    assert first_record["saved_response_path"] is not None
    assert second_record["endpoint_mode"] == "asyncquery"
    assert second_record["submit_status_code"] == submit_status_code
    assert second_record["status_code"] == 200
    assert second_record["result_count"] == 1
    assert second_record["saved_response_path"] is not None


def test_run_benchmark_preserves_error_description_from_wrapper(tmp_path) -> None:
    query_document = {
        "query_name": "failing_query",
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {
                        "categories": ["biolink:ChemicalEntity"],
                        "ids": ["CHEBI:1"],
                    },
                    "n1": {
                        "categories": ["biolink:Disease"],
                    },
                },
                "edges": {
                    "e01": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:treats"],
                    }
                },
            }
        },
        "workflow": [{"id": "lookup"}],
    }
    query_path = tmp_path / "queries.jsonl"
    query_path.write_text(f"{json.dumps(query_document)}\n", encoding="utf-8")
    queries = load_queries([query_path])

    error_response = {
        "status": "UpstreamError",
        "description": "QLever request failed: HTTP Error 429: Too Many Requests",
        "http_code": 502,
    }

    with run_test_server(error_response, status_code=502) as server_info:
        base_url, _requests = server_info
        report = run_benchmark(
            endpoints=[
                {
                    "name": "local",
                    "base_url": base_url,
                    "query_url": build_query_url(base_url),
                }
            ],
            queries=queries,
            iterations=1,
            timeout_seconds=5.0,
        )

    assert report["has_failures"] is True
    record = report["records"][0]
    assert record["status_code"] == 502
    assert (
        record["error"]
        == "HTTP 502: Bad Gateway; QLever request failed: HTTP Error 429: Too Many Requests"
    )


def build_async_callback_payload(node_id: str) -> dict:
    return {
        "status": "Success",
        "description": "Query processed successfully",
        "http_code": 200,
        "message": {
            "knowledge_graph": {
                "nodes": {
                    node_id: {},
                    "MONDO:1": {},
                },
                "edges": {
                    "edge-1": {},
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": [{"id": node_id}],
                        "n1": [{"id": "MONDO:1"}],
                    },
                    "analyses": [{"edge_bindings": {"e01": [{"id": "edge-1"}]}}],
                }
            ],
        },
    }


@contextmanager
def run_test_server(response_body: dict, status_code: int = 200):
    requests: list[dict] = []
    encoded_body = json.dumps(response_body).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers["Content-Length"])
            payload = self.rfile.read(content_length)
            requests.append(
                {
                    "path": self.path,
                    "payload": json.loads(payload.decode("utf-8")),
                }
            )
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded_body)))
            self.end_headers()
            self.wfile.write(encoded_body)

        def log_message(self, format, *args) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", requests
    finally:
        server.shutdown()
        thread.join()


@contextmanager
def run_async_test_server(
    callback_payloads: list[dict],
    *,
    submit_status_code: int = 202,
    submit_status_field: str = "Accepted",
):
    requests: list[dict] = []
    queued_callbacks: list[tuple[str, dict]] = []
    callback_threads: list[threading.Thread] = []

    def deliver_callback(callback_url: str, payload: dict) -> None:
        encoded_payload = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            callback_url,
            data=encoded_payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            response.read()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/asyncquery":
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            content_length = int(self.headers["Content-Length"])
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
            requests.append(
                {
                    "path": self.path,
                    "payload": payload,
                }
            )
            callback_index = len(requests) - 1
            queued_callbacks.append((payload["callback"], callback_payloads[callback_index]))
            if len(queued_callbacks) == len(callback_payloads):
                for callback_url, callback_payload in queued_callbacks:
                    thread = threading.Thread(
                        target=deliver_callback,
                        args=(callback_url, callback_payload),
                        daemon=True,
                    )
                    thread.start()
                    callback_threads.append(thread)
                queued_callbacks.clear()

            response_body = json.dumps(
                {
                    "status": submit_status_field,
                    "description": "Query accepted for asynchronous processing",
                    "http_code": submit_status_code,
                    "job_id": f"job-{callback_index + 1}",
                }
            ).encode("utf-8")
            self.send_response(submit_status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

        def log_message(self, format, *args) -> None:  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", requests
    finally:
        server.shutdown()
        thread.join()
        for callback_thread in callback_threads:
            callback_thread.join()

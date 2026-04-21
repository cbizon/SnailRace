# TRAPI Performance Tester

Standalone TRAPI performance runner derived from the Plater deployment harness.
It posts TRAPI payloads directly to one or more `/query` or `/asyncquery`
endpoints, records request timings and response sizes, and emits structured
JSON output with aggregated summaries.

## Features

- Accepts arbitrary TRAPI endpoint base URLs or direct `/query` or `/asyncquery` URLs
- Loads strict JSONL query sets
- Emits per-request records and aggregated summaries
- Summarizes by endpoint, query, endpoint/query pair, hop count, and pinned node
- Ships with the ported Robokop two-hop query set plus Imatinib-to-asthma 1/2/3-hop queries
- Optionally saves raw TRAPI response bodies
- For `/asyncquery`, submits the full batch first and then collects callback payloads as they arrive

## Usage

List the packaged queries:

```bash
uv run trapi-performance --list-queries
```

Run the packaged queries against one endpoint:

```bash
uv run trapi-performance \
  --endpoint qlever=http://localhost:8000 \
  --output results/qlever.json
```

Run the packaged queries against an async endpoint:

```bash
uv run trapi-performance \
  --endpoint qlever=http://localhost:8000/asyncquery \
  --callback-bind-host 127.0.0.1 \
  --callback-port 8765 \
  --output results/qlever_async.json
```

Run only the Imatinib-to-asthma queries:

```bash
uv run trapi-performance \
  --endpoint qlever=http://localhost:8000 \
  --query-name imatinib_to_asthma_1_hop_related_to_at_instance_level \
  --query-name imatinib_to_asthma_2_hop_related_to_at_instance_level \
  --query-name imatinib_to_asthma_3_hop_related_to_at_instance_level \
  --output results/imatinib_asthma.json
```

Compare multiple endpoints:

```bash
uv run trapi-performance \
  --endpoint qlever=http://localhost:8000 \
  --endpoint robokop=https://automat.ci.transltr.io/robokopkg/ \
  --iterations 3 \
  --output results/comparison.json
```

If the async endpoint cannot reach your local bind address directly, expose the
callback server and advertise that public URL instead:

```bash
uv run trapi-performance \
  --endpoint qlever=https://example.org/trapi/asyncquery \
  --callback-bind-host 0.0.0.0 \
  --callback-port 8765 \
  --callback-base-url https://public-callback.example.org \
  --output results/qlever_async_remote.json
```

`--timeout-seconds` remains a per-request deadline. For `/asyncquery`, that
deadline is measured from submit time until the matching callback arrives.

## Output

The output JSON contains:

- top-level run metadata
- the endpoint catalog
- the query catalog, including pinned-node metadata
- one record per request attempt
- aggregate summaries with numeric stats (`min`, `max`, `mean`, `median`, `p95`)

If any requests fail, the report is still written and the CLI exits with status `1`.

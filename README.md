# TRAPI Performance Tester

Standalone TRAPI performance runner derived from the Plater deployment harness.
It posts TRAPI payloads directly to one or more `/query` endpoints, records
request timings and response sizes, and emits structured JSON output with
aggregated summaries.

## Features

- Accepts arbitrary TRAPI endpoint base URLs or direct `/query` URLs
- Loads strict JSONL query sets
- Emits per-request records and aggregated summaries
- Summarizes by endpoint, query, endpoint/query pair, hop count, and pinned node
- Ships with the ported Robokop two-hop query set plus Imatinib-to-asthma 1/2/3-hop queries
- Optionally saves raw TRAPI response bodies

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

## Output

The output JSON contains:

- top-level run metadata
- the endpoint catalog
- the query catalog, including pinned-node metadata
- one record per request attempt
- aggregate summaries with numeric stats (`min`, `max`, `mean`, `median`, `p95`)

If any requests fail, the report is still written and the CLI exits with status `1`.

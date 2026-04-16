from __future__ import annotations

import json

import pytest

from trapi_performance_tester.query_io import default_query_paths, load_queries


def test_default_query_paths_include_packaged_sets() -> None:
    names = [path.name for path in default_query_paths()]
    assert names == [
        "imatinib_asthma_hops.jsonl",
        "robokop_two_hop_trapi.jsonl",
    ]


def test_default_query_paths_excludes_kara_templates() -> None:
    names = [path.name for path in default_query_paths()]
    assert not any(name.startswith("kara_") for name in names)


def test_load_queries_extracts_request_body_and_metadata(tmp_path) -> None:
    query_document = {
        "query_name": "simple_query",
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

    assert len(queries) == 1
    query = queries[0]
    assert query["query_name"] == "simple_query"
    assert "query_name" not in query["request_body"]
    assert query["metadata"]["hop_count"] == 1
    assert query["metadata"]["pinned_node_ids"] == ["CHEBI:1"]
    assert query["metadata"]["edge_predicates"] == ["biolink:treats"]
    assert query["metadata"]["node_categories"]["n1"] == ["biolink:Disease"]


def _make_template_query(query_name: str = "template_query") -> dict:
    return {
        "query_name": query_name,
        "message": {
            "query_graph": {
                "nodes": {
                    "$source": {
                        "ids": ["$source_id"],
                        "categories": ["biolink:ChemicalEntity"],
                    },
                    "$target": {
                        "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                    },
                    "g": {"categories": ["biolink:Gene"]},
                },
                "edges": {
                    "edge_0": {
                        "subject": "$source",
                        "object": "$target",
                        "predicates": ["biolink:treats"],
                    }
                },
            }
        },
        "workflow": [{"id": "lookup"}],
    }


def test_load_queries_substitutes_source_id_in_template(tmp_path) -> None:
    query_path = tmp_path / "templates.jsonl"
    query_path.write_text(f"{json.dumps(_make_template_query())}\n", encoding="utf-8")

    queries = load_queries([query_path], source_id="CHEBI:45783")

    assert len(queries) == 1
    source_node = queries[0]["request_body"]["message"]["query_graph"]["nodes"]["$source"]
    assert source_node["ids"] == ["CHEBI:45783"]


def test_load_queries_raises_when_template_loaded_without_source_id(tmp_path) -> None:
    query_path = tmp_path / "templates.jsonl"
    query_path.write_text(f"{json.dumps(_make_template_query())}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="requires --source-id"):
        load_queries([query_path])


def test_load_queries_rejects_invalid_json(tmp_path) -> None:
    query_path = tmp_path / "queries.jsonl"
    query_path.write_text("{not-json}\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"queries\.jsonl:1"):
        load_queries([query_path])


def test_packaged_imatinib_asthma_queries_have_expected_shape() -> None:
    query_names = {
        "imatinib_to_asthma_1_hop_related_to_at_instance_level",
        "imatinib_to_asthma_2_hop_related_to_at_instance_level",
        "imatinib_to_asthma_3_hop_related_to_at_instance_level",
    }

    queries = load_queries(default_query_paths(), include_names=query_names)
    assert {query["query_name"] for query in queries} == query_names

    queries_by_name = {query["query_name"]: query for query in queries}
    assert (
        queries_by_name["imatinib_to_asthma_1_hop_related_to_at_instance_level"]["metadata"]["hop_count"]
        == 1
    )
    assert (
        queries_by_name["imatinib_to_asthma_2_hop_related_to_at_instance_level"]["metadata"]["hop_count"]
        == 2
    )
    assert (
        queries_by_name["imatinib_to_asthma_3_hop_related_to_at_instance_level"]["metadata"]["hop_count"]
        == 3
    )

    for query in queries:
        metadata = query["metadata"]
        assert metadata["edge_predicates"] == ["biolink:related_to_at_instance_level"]
        assert metadata["pinned_node_ids"] == ["CHEBI:45783", "MONDO:0004979"]

    two_hop_nodes = queries_by_name[
        "imatinib_to_asthma_2_hop_related_to_at_instance_level"
    ]["request_body"]["message"]["query_graph"]["nodes"]
    assert two_hop_nodes["n1"]["categories"] == ["biolink:NamedThing"]

    three_hop_nodes = queries_by_name[
        "imatinib_to_asthma_3_hop_related_to_at_instance_level"
    ]["request_body"]["message"]["query_graph"]["nodes"]
    assert three_hop_nodes["n1"]["categories"] == ["biolink:NamedThing"]
    assert three_hop_nodes["n2"]["categories"] == ["biolink:NamedThing"]

from __future__ import annotations

import ibis
import pytest

from ibis_hotdata.vector import (
    cosine_distance,
    l2_distance,
    negative_dot_product,
    semantic_search,
)

ITEMS = ibis.table(
    {"id": "int64", "name": "string", "embedding": "array<float32>"},
    name="items",
)


@pytest.mark.parametrize(
    ("fn", "expected_call"),
    [
        (cosine_distance, "COSINE_DISTANCE"),
        (l2_distance, "L2_DISTANCE"),
        (negative_dot_product, "NEGATIVE_DOT_PRODUCT"),
    ],
)
def test_distance_udf_compiles_to_function_call(fn, expected_call):
    expr = ITEMS.select(dist=fn(ITEMS.embedding, ibis.literal([0.1, 0.2, 0.3])))
    sql = ibis.to_sql(expr, dialect="postgres")
    assert f'{expected_call}("t0"."embedding", ARRAY[0.1, 0.2, 0.3])' in sql


def test_semantic_search_query_vector_is_a_literal_array():
    expr = semantic_search(ITEMS, "embedding", [0.1, 0.2, 0.3], k=5)
    sql = ibis.to_sql(expr, dialect="postgres")
    assert "ARRAY[0.1, 0.2, 0.3]" in sql


def test_semantic_search_aliases_distance_and_excludes_source_column():
    expr = semantic_search(ITEMS, "embedding", [0.1, 0.2, 0.3], k=5)
    sql = ibis.to_sql(expr, dialect="postgres")
    assert 'AS "distance"' in sql
    # The embedding column is a valid distance-function argument, but must not
    # appear as an output column (engine issue #508: vector-in-output disables
    # the HNSW fast path).
    assert set(expr.columns) == {"id", "name", "distance"}


def test_semantic_search_orders_ascending_with_limit():
    expr = semantic_search(ITEMS, "embedding", [0.1, 0.2, 0.3], k=5)
    sql = ibis.to_sql(expr, dialect="postgres")
    assert "ORDER BY" in sql
    assert '"distance" ASC' in sql
    assert "LIMIT 5" in sql


def test_semantic_search_honors_custom_distance_fn_and_name():
    expr = semantic_search(
        ITEMS,
        "embedding",
        [0.1, 0.2, 0.3],
        k=10,
        distance_fn=l2_distance,
        distance_name="score",
    )
    sql = ibis.to_sql(expr, dialect="postgres")
    assert "L2_DISTANCE" in sql
    assert 'AS "score"' in sql
    assert '"score" ASC' in sql
    assert "LIMIT 10" in sql


def test_semantic_search_accepts_column_expression():
    expr = semantic_search(ITEMS, ITEMS.embedding, [0.1, 0.2, 0.3], k=5)
    assert "embedding" not in expr.columns

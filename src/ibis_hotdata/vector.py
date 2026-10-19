"""Vector-search helpers: distance UDFs + a semantic-search query builder.

Bridges Hotdata's vector-indexed (HNSW / IVF) distance functions into Ibis. These are pure
functions over Ibis expressions built on ``@ibis.udf.scalar.builtin`` — no
backend changes required, and no Python is shipped to the engine.
"""

from __future__ import annotations

from collections.abc import Sequence

import ibis
import ibis.expr.types as ir


@ibis.udf.scalar.builtin
def cosine_distance(a, b) -> float:
    """Cosine distance between two vectors."""


@ibis.udf.scalar.builtin
def l2_distance(a, b) -> float:
    """Euclidean (L2) distance between two vectors."""


@ibis.udf.scalar.builtin
def negative_dot_product(a, b) -> float:
    """Negative dot product between two vectors (smaller is more similar)."""


@ibis.udf.scalar.builtin
def vector_distance(a, b) -> float:
    """Distance between a text column with an embedding-backed index and a text query.

    Hotdata embeds the query with the index's provider and uses the index's metric.
    """


def semantic_search(
    table: ir.Table,
    column: str | ir.Column,
    query_vector: Sequence[float] | str,
    k: int,
    *,
    distance_fn=cosine_distance,
    distance_name: str = "distance",
) -> ir.Table:
    """Return the `k` rows of `table` whose `column` is nearest `query_vector`.

    With a vector query, excludes `column` from the result and orders
    ascending by `distance_fn(column, query_vector)`, aliased as `distance_name`.

    With a text query (a `str`), `column` is the source text column of an
    embedding-backed index: orders by `vector_distance(column, query)`
    (`distance_fn` is ignored) and excludes every array (vector) column, so
    the generated embedding column is left out of the result.

    `distance_name` must not collide with an existing column name in `table`,
    or `select` raises a duplicate-column error.
    """
    col = table[column] if isinstance(column, str) else column
    if isinstance(query_vector, str):
        schema = table.schema()
        other_cols = [name for name in table.columns if not schema[name].is_array()]
        dist = vector_distance(col, ibis.literal(query_vector))
    else:
        other_cols = [name for name in table.columns if name != col.get_name()]
        dist = distance_fn(col, ibis.literal(list(query_vector)))
    return (
        table.select(*other_cols, **{distance_name: dist})
        .order_by(ibis.asc(distance_name))
        .limit(k)
    )

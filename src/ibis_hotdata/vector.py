"""Vector-search helpers: distance UDFs + a semantic-search query builder.

Bridges Hotdata's HNSW-indexed distance functions into Ibis. These are pure
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


def semantic_search(
    table: ir.Table,
    column: str | ir.ArrayColumn,
    query_vector: Sequence[float],
    k: int,
    *,
    distance_fn=cosine_distance,
    distance_name: str = "distance",
) -> ir.Table:
    """Return the `k` rows of `table` whose `column` is nearest `query_vector`.

    Excludes `column` from the result and orders ascending by
    `distance_fn(column, query_vector)`, aliased as `distance_name`.
    """
    col = table[column] if isinstance(column, str) else column
    other_cols = [name for name in table.columns if name != col.get_name()]
    qvec = ibis.literal(list(query_vector))
    return (
        table.select(*other_cols, **{distance_name: distance_fn(col, qvec)})
        .order_by(ibis.asc(distance_name))
        .limit(k)
    )

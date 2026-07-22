from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("hotdata-ibis")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

from ibis_hotdata.backend import Backend
from ibis_hotdata.vector import (
    cosine_distance,
    l2_distance,
    negative_dot_product,
    semantic_search,
)

__all__ = [
    "Backend",
    "__version__",
    "cosine_distance",
    "l2_distance",
    "negative_dot_product",
    "semantic_search",
]

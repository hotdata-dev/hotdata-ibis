from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ibis-hotdata")
except PackageNotFoundError:
    __version__ = "0.0.0+unknown"

from ibis_hotdata.backend import Backend

__all__ = ["Backend", "__version__"]

import re

from importlib.metadata import version as dist_version

import ibis_hotdata


def test_version_is_pep440_core():
    assert re.fullmatch(r"\d+\.\d+\.\d+(\+.*)?", ibis_hotdata.__version__)


def test_version_matches_distribution_metadata():
    assert dist_version("ibis-hotdata") == ibis_hotdata.__version__

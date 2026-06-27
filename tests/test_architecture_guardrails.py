from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_dependencies_do_not_include_hotdata_framework() -> None:
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r"(?ms)^dependencies\s*=\s*\[(.*?)\]", pyproject_text)
    assert match is not None
    dependencies_block = match.group(1)
    assert "hotdata-framework" not in dependencies_block
    assert "hotdata_framework" not in dependencies_block


def test_source_tree_does_not_import_hotdata_framework() -> None:
    violations: list[str] = []
    import_patterns = (
        re.compile(r"(?m)^\s*from\s+hotdata_framework(?:\.|\s+import)"),
        re.compile(r"(?m)^\s*import\s+hotdata_framework(?:\s|$|,)"),
    )

    for folder in ("src", "tests", "examples"):
        for path in (REPO_ROOT / folder).rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if any(pattern.search(text) for pattern in import_patterns):
                violations.append(str(path.relative_to(REPO_ROOT)))

    assert not violations, (
        "hotdata-ibis must remain independent from hotdata-framework; "
        f"found forbidden imports in: {', '.join(violations)}"
    )

"""Smoke tests for the Zeeker resource collection.

Every resource module must import cleanly and expose the entry points that
zeeker's builder relies on:

- ``fetch_data`` — required on every resource declared in ``zeeker.toml``.
- ``fetch_fragments_data`` — additionally required when the resource opts in
  with ``fragments = true`` in its ``zeeker.toml`` section.

These tests deliberately mirror how zeeker loads resources (the ``resources/``
directory is appended to ``sys.path`` and modules are imported by plain file
name, so intra-package imports like ``from _token_usage import ...`` resolve).
They perform no network I/O, so they are cheap and safe to run on every push
and pull request.
"""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path
from types import ModuleType
from typing import Any, Dict

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESOURCES_DIR = PROJECT_ROOT / "resources"
ZEEKER_TOML_PATH = PROJECT_ROOT / "zeeker.toml"

# zeeker's builder appends resources/ to sys.path before importing resource
# modules; resource modules rely on this for sibling imports.
sys.path.append(str(RESOURCES_DIR))

_loaded_modules: Dict[str, ModuleType] = {}


def _resource_py_files() -> list[Path]:
    """All Python modules in resources/, excluding the package __init__."""
    if not RESOURCES_DIR.is_dir():
        return []
    return sorted(p for p in RESOURCES_DIR.glob("*.py") if p.stem != "__init__")


def _configured_resources() -> Dict[str, Dict[str, Any]]:
    """Resource sections from zeeker.toml, keyed by resource name."""
    try:
        raw = tomllib.loads(ZEEKER_TOML_PATH.read_text("utf-8"))
    except FileNotFoundError:
        return {}
    resources = raw.get("resource")
    if not isinstance(resources, dict):
        return {}
    return {name: cfg if isinstance(cfg, dict) else {} for name, cfg in resources.items()}


def _load_resource(path: Path) -> ModuleType:
    """Import a resource module the same way zeeker's builder does."""
    name = path.stem
    if name in _loaded_modules:
        return _loaded_modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not build an import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    _loaded_modules[name] = module
    return module


def test_resources_directory_exists_and_is_not_empty() -> None:
    """Fail loudly when there are no resources to build at all."""
    assert RESOURCES_DIR.is_dir(), f"Resources directory is missing: {RESOURCES_DIR}"
    files = _resource_py_files()
    assert files, (
        f"No resource modules found in {RESOURCES_DIR} — the database would be "
        "built empty. Expected at least one resource module (e.g. mlaw_news.py)."
    )


def test_every_configured_resource_module_file_exists() -> None:
    """Every [resource.<name>] in zeeker.toml must have resources/<name>.py.

    A configured resource without its module fails at build time only after
    discovery work has run; catching it here surfaces the breakage in CI.
    """
    configured = _configured_resources()
    assert configured, f"No [resource.*] sections found in {ZEEKER_TOML_PATH}"
    available = {p.stem for p in _resource_py_files()}
    missing = sorted(set(configured) - available)
    assert not missing, (
        f"zeeker.toml declares resources that have no module in " f"{RESOURCES_DIR}: {missing}"
    )


@pytest.mark.parametrize("path", _resource_py_files(), ids=lambda p: p.stem)
def test_resource_module_imports_cleanly(path: Path) -> None:
    """Import every module in resources/. Import errors fail the build."""
    _load_resource(path)


@pytest.mark.parametrize("name", sorted(_configured_resources()), ids=lambda n: n)
def test_fetch_data_is_callable(name: str) -> None:
    """Every configured resource must expose a callable fetch_data()."""
    module = _load_resource(RESOURCES_DIR / f"{name}.py")
    fetch_data = getattr(module, "fetch_data", None)
    assert callable(fetch_data), (
        f"resources/{name}.py has no callable fetch_data() — zeeker's builder "
        "will reject this resource at build time"
    )


def test_fragment_enabled_resources_expose_fetch_fragments_data() -> None:
    """Resources with fragments = true must expose a callable fetch_fragments_data()."""
    enabled = {name: cfg for name, cfg in _configured_resources().items() if cfg.get("fragments")}
    if not enabled:
        pytest.skip("no fragment-enabled resources configured in zeeker.toml")
    for name in sorted(enabled):
        module = _load_resource(RESOURCES_DIR / f"{name}.py")
        fetch_fragments_data = getattr(module, "fetch_fragments_data", None)
        assert callable(fetch_fragments_data), (
            f"resources/{name}.py has fragments = true in zeeker.toml but no callable "
            "fetch_fragments_data() — zeeker's builder will reject this resource "
            "when it runs the fragments phase"
        )


def test_fetch_fragments_data_only_on_fragment_enabled_resources() -> None:
    """A module defining fetch_fragments_data without fragments = true in
    zeeker.toml is dead code: the builder never runs its fragments phase."""
    enabled = {name for name, cfg in _configured_resources().items() if cfg.get("fragments")}
    for name in sorted(_configured_resources()):
        module = _load_resource(RESOURCES_DIR / f"{name}.py")
        if callable(getattr(module, "fetch_fragments_data", None)) and name not in enabled:
            raise AssertionError(
                f"resources/{name}.py defines fetch_fragments_data() but zeeker.toml "
                "does not set fragments = true for it, so its fragments phase never "
                "runs. Add fragments = true to [resource." + name + "] or remove the "
                "unused function."
            )

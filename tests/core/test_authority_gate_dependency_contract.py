from __future__ import annotations

import importlib
from pathlib import Path
import re
import tomllib


REPO_ROOT = Path(__file__).resolve().parents[2]
CORE_PYPROJECT = REPO_ROOT / "hfa-core" / "pyproject.toml"
AUTHORITY_WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "ci-authority-gate.yml"
)


def test_authority_gate_uses_declared_hfa_core_dependency_contract():
    project = tomllib.loads(
        CORE_PYPROJECT.read_text(encoding="utf-8")
    )
    dependencies = project["project"]["dependencies"]
    assert dependencies.count("rfc8785==0.1.4") == 1

    workflow = AUTHORITY_WORKFLOW.read_text(encoding="utf-8-sig")
    assert "python -m pip install -e hfa-core" in workflow
    assert (
        "tests/core/test_authority_gate_dependency_contract.py"
        in workflow
    )

    standalone_install = re.compile(
        r"^\s*python\s+-m\s+pip\s+install\s+rfc8785(?:\s|$)",
        re.MULTILINE,
    )
    assert standalone_install.search(workflow) is None

    module = importlib.import_module("rfc8785")
    assert module.__version__ == "0.1.4"

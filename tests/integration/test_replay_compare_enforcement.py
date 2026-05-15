from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "replay_compare.py"
_SPEC = importlib.util.spec_from_file_location("replay_compare", _SCRIPT)
assert _SPEC and _SPEC.loader

replay_compare = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = replay_compare
_SPEC.loader.exec_module(replay_compare)


def test_replay_compare_clean_result_is_zero():
    result = replay_compare.evaluate_replay_compare()
    assert result.replay_clean is True
    assert result.deterministic_replay_ok is True
    assert result.exit_code == 0


def test_replay_compare_integrity_issue_returns_nonzero():
    result = replay_compare.evaluate_replay_compare(integrity_issue=True)
    assert result.exit_code == 1
    assert result.ambiguous is True
    assert "integrity_issue" in result.reason


def test_replay_compare_cli_nonzero_on_mismatch():
    assert replay_compare.main(["--mismatch"]) == 1

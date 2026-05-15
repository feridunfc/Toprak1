from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "full_auto_v3_2_smoke.py"
_SPEC = importlib.util.spec_from_file_location("full_auto_v3_2_smoke", _SCRIPT)
assert _SPEC and _SPEC.loader

smoke = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = smoke
_SPEC.loader.exec_module(smoke)


def test_smoke_script_lists_integrated_tests() -> None:
    assert len(smoke.SMOKE_TESTS) >= 16
    assert "tests/core/test_event_store_core.py" in smoke.SMOKE_TESTS
    assert "tests/integration/test_phase6a_runtime_safety.py" in smoke.SMOKE_TESTS


def test_smoke_script_builds_pytest_command() -> None:
    command = smoke.build_command(["-k", "nothing"])
    assert command[0] == sys.executable
    assert command[1:3] == ["-m", "pytest"]
    assert "-q" in command
    assert "--tb=short" in command
    assert "-k" in command


def test_smoke_script_dry_run_returns_zero() -> None:
    repo = Path(__file__).resolve().parents[2]
    assert smoke.main(["--repo-root", str(repo), "--dry-run"]) == 0


def test_smoke_script_fakeredis_env_mode() -> None:
    env = smoke.build_env(redis_mode="fakeredis")
    assert env["USE_FAKE_REDIS"] == "1"
    assert env["USE_EXISTING_REDIS"] == "0"
    assert env["REDIS_URL"].startswith("fakeredis://")

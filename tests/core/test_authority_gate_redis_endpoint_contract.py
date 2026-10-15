"""Authority Gate must use its declared CI service before any Redis reset."""
from __future__ import annotations

import ast
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/ci-authority-gate.yml"
URL = "redis://127.0.0.1:6389/0"
INITIAL = "Verify CI Redis service before Redis-backed tests"
ARTIFACTS = "Verify CI Redis service before Redis-backed artifacts"
RESETS = (
    "Reset integration Redis before thin product CLI tests",
    "Reset integration Redis before HTTP and payload tests",
)
SERVICE_STEPS = (INITIAL, *RESETS, ARTIFACTS)


def job():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8-sig"))["jobs"]["authority-gate"]


def run_service_step(name):
    step = next(step for step in job()["steps"] if step.get("name") == name)
    lines = step["run"].splitlines()
    assert lines[0] == "python - <<'PY'" and lines[-1] == "PY"
    exec(compile("\n".join(lines[1:-1]), f"{WORKFLOW}:{name}", "exec"), {})


@pytest.fixture
def service(monkeypatch, tmp_path):
    calls = []

    class Client:
        run_id = "ci-owned-service"
        available = True

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def ping(self):
            calls.append("ping")
            if not self.available:
                raise ConnectionError("service unavailable")
            return True

        def info(self, section):
            calls.append("info")
            if not self.available:
                raise ConnectionError("service unavailable")
            return {"run_id": self.run_id}

        def flushdb(self):
            calls.append("flushdb")

    client = Client()

    def connect(url, **kwargs):
        calls.append("connect")
        assert url == URL
        assert kwargs["socket_connect_timeout"] > 0 and kwargs["socket_timeout"] > 0
        return client

    monkeypatch.setitem(sys.modules, "redis", SimpleNamespace(Redis=SimpleNamespace(from_url=connect)))
    monkeypatch.setenv("REDIS_URL", URL)
    monkeypatch.setenv("USE_EXISTING_REDIS", "1")
    monkeypatch.setenv("AUTHORITY_GATE_REDIS_RUN_ID", client.run_id)
    exports = tmp_path / "github-env"
    monkeypatch.setenv("GITHUB_ENV", str(exports))
    return client, calls, exports


def test_declared_service_matches_unmodified_integration_fixture():
    fixture = ast.parse((ROOT / "tests/integration/conftest.py").read_text())
    fixture_url = next(
        node.value.value for node in fixture.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "REDIS_URL" for target in node.targets)
    )
    config = job()
    assert config["env"]["REDIS_URL"] == fixture_url == URL
    assert config["env"]["USE_EXISTING_REDIS"] == "1"
    assert config["services"]["redis"]["ports"] == ["6389:6379"]
    for step in config["steps"]:
        env = step.get("env", {})
        assert env.get("REDIS_URL", URL) == URL
        assert env.get("USE_EXISTING_REDIS", "1") == "1"
        for line in step.get("run", "").splitlines():
            if "--redis-url" in line:
                assert '--redis-url "$REDIS_URL"' in line


def test_artifact_check_reuses_service_and_preserves_independent_smoke():
    steps = {step.get("name"): step for step in job()["steps"]}
    assert "docker compose" not in steps[ARTIFACTS]["run"]
    assert "flushdb" not in steps[ARTIFACTS]["run"]
    smoke = steps["Generate deployment smoke artifacts"]
    assert smoke["env"]["REDIS_SMOKE_PORT"] == "6390"
    assert "scripts/deployment_smoke.py --json" in smoke["run"]
    assert "scripts/redis_failover_smoke.py --json" in smoke["run"]
    assert steps["Upload Authority Gate evidence artifacts"]["with"]["if-no-files-found"] == "error"


def test_initial_health_exports_identity_only_after_ping_and_reset(service):
    client, calls, exports = service
    run_service_step(INITIAL)
    assert calls == ["connect", "ping", "info", "flushdb"]
    assert exports.read_text() == f"AUTHORITY_GATE_REDIS_RUN_ID={client.run_id}\n"


@pytest.mark.parametrize("name", SERVICE_STEPS)
def test_endpoint_mismatch_refuses_before_connect_or_reset(name, service, monkeypatch):
    _, calls, exports = service
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    with pytest.raises(SystemExit, match="requires its CI-owned Redis service"):
        run_service_step(name)
    assert calls == [] and not exports.exists()


@pytest.mark.parametrize("name", SERVICE_STEPS)
def test_unavailable_service_is_an_error_without_reset(name, service):
    client, calls, exports = service
    client.available = False
    with pytest.raises(ConnectionError, match="service unavailable"):
        run_service_step(name)
    assert "flushdb" not in calls and not exports.exists()


@pytest.mark.parametrize("name", (*RESETS, ARTIFACTS))
def test_changed_service_identity_refuses_before_reset(name, service, monkeypatch):
    _, calls, exports = service
    monkeypatch.setenv("AUTHORITY_GATE_REDIS_RUN_ID", "different-service")
    with pytest.raises(SystemExit, match="service identity changed"):
        run_service_step(name)
    assert "flushdb" not in calls and not exports.exists()


def test_missing_endpoint_does_not_select_a_default(service, monkeypatch):
    _, calls, _ = service
    monkeypatch.delenv("REDIS_URL")
    with pytest.raises(KeyError, match="REDIS_URL"):
        run_service_step(INITIAL)
    assert calls == []

from __future__ import annotations

import pytest

from hfa_worker import process_root


def _env(**overrides) -> dict[str, str]:
    env = {
        "WORKER_EXECUTOR_MODE": "fake",
        "WORKER_ID": "worker-c2-root",
    }
    env.update(overrides)
    return env


def test_process_root_terminal_binding_defaults_false() -> None:
    config = process_root.config_from_env(_env())
    assert config["canonical_task_terminal_binding"] is False


@pytest.mark.parametrize("value", ["true", "1", "on"])
def test_process_root_terminal_binding_true_values(value: str) -> None:
    config = process_root.config_from_env(
        _env(HFA_CANONICAL_TASK_TERMINAL_BINDING=value)
    )
    assert config["canonical_task_terminal_binding"] is True


@pytest.mark.parametrize("value", ["false", "0", "off"])
def test_process_root_terminal_binding_false_values(value: str) -> None:
    config = process_root.config_from_env(
        _env(HFA_CANONICAL_TASK_TERMINAL_BINDING=value)
    )
    assert config["canonical_task_terminal_binding"] is False


def test_process_root_terminal_binding_rejects_invalid_value() -> None:
    with pytest.raises(RuntimeError, match="HFA_CANONICAL_TASK_TERMINAL_BINDING"):
        process_root.config_from_env(
            _env(HFA_CANONICAL_TASK_TERMINAL_BINDING="maybe")
        )


@pytest.mark.asyncio
async def test_process_root_passes_terminal_binding_to_worker_service(monkeypatch) -> None:
    config = process_root.config_from_env(
        _env(
            HFA_CANONICAL_TASK_ADMIT_BINDING="true",
            HFA_CANONICAL_TASK_DISPATCH_BINDING="true",
            HFA_CANONICAL_TASK_CLAIM_BINDING="true",
            HFA_CANONICAL_TASK_TERMINAL_BINDING="true",
        )
    )
    seen: dict = {}

    class RedisProbe:
        async def aclose(self) -> None:
            return None

    class ServiceProbe:
        def __init__(self, redis, received) -> None:
            seen.update(received)

        async def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

    async def wait_for_shutdown() -> None:
        return None

    monkeypatch.setattr(process_root, "WorkerService", ServiceProbe)
    await process_root.run_worker_process(
        redis_factory=lambda _url: RedisProbe(),
        config=config,
        wait_for_shutdown=wait_for_shutdown,
    )

    assert seen["canonical_task_terminal_binding"] is True
    assert seen["canonical_task_claim_binding"] is True

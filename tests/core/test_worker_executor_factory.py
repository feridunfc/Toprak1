import pytest

from hfa_worker.executor_factory import build_executor
from hfa_worker.fake_executor import FakeExecutor


def test_factory_returns_fake_executor_for_dict_config():
    executor = build_executor({"executor_mode": "fake"})
    assert isinstance(executor, FakeExecutor)


def test_factory_returns_fake_executor_for_object_config():
    class Config:
        executor_mode = "fake"

    executor = build_executor(Config())
    assert isinstance(executor, FakeExecutor)


def test_factory_raises_on_invalid_mode():
    with pytest.raises(ValueError, match="Unsupported executor_mode"):
        build_executor({"executor_mode": "bogus"})
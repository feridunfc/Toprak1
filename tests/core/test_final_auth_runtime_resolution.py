
import os
from fastapi import HTTPException
from hfa_control import auth


def test_auth_runtime_env_resolution(monkeypatch):
    monkeypatch.delenv("CP_AUTH_SECRET", raising=False)
    monkeypatch.setenv("APP_ENV", "development")
    auth.require_operator("anything")

    monkeypatch.setenv("APP_ENV", "production")
    try:
        auth.require_operator("anything")
    except HTTPException as exc:
        assert exc.status_code == 503
    else:
        raise AssertionError("expected 503 in production without secret")


def test_auth_status_runtime_secret(monkeypatch):
    monkeypatch.delenv("CP_AUTH_SECRET", raising=False)
    assert auth.auth_status()["operator_auth_enabled"] is False

    monkeypatch.setenv("CP_AUTH_SECRET", "x" * 32)
    status = auth.auth_status()
    assert status["operator_auth_enabled"] is True
    assert status.secret_length == 32

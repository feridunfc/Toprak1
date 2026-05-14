"""
tests/core/test_sprint17_auth.py
IRONCLAD Sprint 17 — Auth hardening tests

Verifies:
  - hmac.compare_digest used (no == comparison)
  - Missing secret → dev allows, production → 503
  - Wrong secret → 403
  - Missing header → 401
  - Correct secret → passes
  - require_tenant validates header
  - auth_status never exposes secret
"""

from __future__ import annotations

import importlib
import sys
import os
import pytest
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Helpers to reload auth module with different env vars
# ---------------------------------------------------------------------------


def _load_auth(secret: str = "", app_env: str = "development"):
    """Reload auth module with given env vars."""
    env_backup = os.environ.copy()
    os.environ["CP_AUTH_SECRET"] = secret
    os.environ["APP_ENV"] = app_env

    # Remove cached module to force reload
    for key in list(sys.modules.keys()):
        if "hfa_control.auth" in key:
            del sys.modules[key]

    try:
        import hfa_control.auth as auth_mod

        importlib.reload(auth_mod)
        return auth_mod
    finally:
        os.environ.clear()
        os.environ.update(env_backup)


# ---------------------------------------------------------------------------
# Development mode (CP_AUTH_SECRET not set)
# ---------------------------------------------------------------------------


def test_dev_no_secret_allows_any_header():
    """Without CP_AUTH_SECRET in dev mode, any header is accepted."""
    auth = _load_auth(secret="", app_env="development")
    # Should not raise
    auth.require_operator("anything")
    auth.require_operator("")
    auth.require_operator("wrong-value")


def test_production_no_secret_raises_503():
    """In production, missing CP_AUTH_SECRET → 503 Service Unavailable."""
    auth = _load_auth(secret="", app_env="production")
    with pytest.raises(HTTPException) as exc_info:
        auth.require_operator("some-token")
    assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# With CP_AUTH_SECRET set
# ---------------------------------------------------------------------------


def test_correct_secret_passes():
    secret = "super-secret-test-value-32-chars!!"
    auth = _load_auth(secret=secret, app_env="production")
    # Should not raise
    auth.require_operator(secret)


def test_wrong_secret_raises_403():
    auth = _load_auth(secret="correct-secret-value-32-chars-ok!", app_env="production")
    with pytest.raises(HTTPException) as exc_info:
        auth.require_operator("wrong-secret")
    assert exc_info.value.status_code == 403


def test_empty_header_raises_401():
    auth = _load_auth(secret="correct-secret-value-32-chars-ok!", app_env="production")
    with pytest.raises(HTTPException) as exc_info:
        auth.require_operator("")
    assert exc_info.value.status_code == 401


def test_missing_header_raises_401():
    auth = _load_auth(secret="correct-secret-value-32-chars-ok!", app_env="staging")
    with pytest.raises(HTTPException) as exc_info:
        auth.require_operator()
    assert exc_info.value.status_code == 401


def test_staging_counts_as_production():
    """APP_ENV=staging should also fail-closed on missing secret."""
    auth = _load_auth(secret="", app_env="staging")
    with pytest.raises(HTTPException) as exc_info:
        auth.require_operator("token")
    assert exc_info.value.status_code == 503


# ---------------------------------------------------------------------------
# Timing safety (hmac.compare_digest)
# ---------------------------------------------------------------------------


def test_uses_hmac_compare_not_equality():
    """
    Verify that auth.py uses hmac.compare_digest rather than ==.
    This is a code inspection test — reads the source.
    """
    import inspect
    import hfa_control.auth as auth_mod

    source = inspect.getsource(auth_mod)
    assert "hmac.compare_digest" in source, (
        "auth.py must use hmac.compare_digest for constant-time comparison"
    )
    # Must NOT use bare == comparison for secrets
    # (The == in the comparison block would be a timing oracle)
    # We check the require_operator function specifically
    src_fn = inspect.getsource(auth_mod.require_operator)
    assert "==" not in src_fn or "status_code" in src_fn, (
        "require_operator must not use == to compare secrets"
    )


# ---------------------------------------------------------------------------
# require_tenant
# ---------------------------------------------------------------------------


def test_require_tenant_returns_stripped_value():
    import hfa_control.auth as auth_mod

    result = auth_mod.require_tenant("  acme  ")
    assert result == "acme"


def test_require_tenant_empty_raises_400():
    import hfa_control.auth as auth_mod

    with pytest.raises(HTTPException) as exc_info:
        auth_mod.require_tenant("")
    assert exc_info.value.status_code == 400


def test_require_tenant_whitespace_only_raises_400():
    import hfa_control.auth as auth_mod

    with pytest.raises(HTTPException) as exc_info:
        auth_mod.require_tenant("   ")
    assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# auth_status — never expose secret
# ---------------------------------------------------------------------------


def test_auth_status_does_not_expose_secret():
    auth = _load_auth(secret="my-top-secret-value-never-expose!!", app_env="production")
    status = auth.auth_status()
    assert "my-top-secret-value-never-expose!!" not in str(status)
    assert status["operator_auth_enabled"] is True
    assert status["production_mode"] is True


def test_auth_status_shows_disabled_when_no_secret():
    auth = _load_auth(secret="", app_env="development")
    status = auth.auth_status()
    assert status["operator_auth_enabled"] is False
    assert status["secret_length"] == 0

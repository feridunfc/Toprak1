
from __future__ import annotations

import hmac
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import HTTPException

logger = logging.getLogger(__name__)
_STRICT_ENVS = {"production", "staging"}


@dataclass(frozen=True)
class AuthStatus:
    enabled: bool
    mode: str
    app_env: str
    strict: bool
    secret_length: int

    @property
    def operator_auth_enabled(self) -> bool:
        return self.enabled

    @property
    def production_mode(self) -> bool:
        return self.strict

    @property
    def auth_mode(self) -> str:
        return "shared_secret"

    def to_dict(self) -> dict[str, Any]:
        return {
            "operator_auth_enabled": self.operator_auth_enabled,
            "app_env": self.app_env,
            "production_mode": self.production_mode,
            "secret_length": self.secret_length,
            "auth_mode": self.auth_mode,
            "mode": self.mode,
            "strict": self.strict,
            "enabled": self.enabled,
        }

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]


@dataclass(frozen=True)
class AuthDecision:
    allowed: bool
    reason: str


def _get_app_env() -> str:
    return (os.getenv("APP_ENV") or os.getenv("ENVIRONMENT") or "development").strip().lower()


def _is_strict_env() -> bool:
    return _get_app_env() in _STRICT_ENVS


def _get_secret() -> Optional[str]:
    raw = os.getenv("CP_AUTH_SECRET")
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def auth_status() -> AuthStatus:
    secret = _get_secret()
    return AuthStatus(
        enabled=secret is not None,
        mode="shared-secret-header",
        app_env=_get_app_env(),
        strict=_is_strict_env(),
        secret_length=len(secret) if secret is not None else 0,
    )


def _ensure_secret_available() -> Optional[bytes]:
    secret = _get_secret()
    if secret is None:
        if _is_strict_env():
            logger.critical(
                "SECURITY: CP_AUTH_SECRET missing in %s. Failing closed.",
                _get_app_env().upper(),
            )
            return None
        logger.warning(
            "CP_AUTH_SECRET missing (APP_ENV=%s). Development/test open mode enabled.",
            _get_app_env(),
        )
        return None
    return secret.encode("utf-8")


def validate_shared_secret_header(header_value: Optional[str]) -> bool:
    secret = _ensure_secret_available()

    if secret is None:
        return not _is_strict_env()

    if header_value is None:
        return False

    supplied = header_value.strip().encode("utf-8")
    if not supplied:
        return False

    return hmac.compare_digest(supplied, secret)


def verify_shared_secret(header_value: Optional[str]) -> AuthDecision:
    ok = validate_shared_secret_header(header_value)
    if ok:
        if _get_secret() is None:
            return AuthDecision(True, "development_open_mode")
        return AuthDecision(True, "shared_secret_match")
    if _get_secret() is None and _is_strict_env():
        return AuthDecision(False, "missing_secret_fail_closed")
    if not (header_value or "").strip():
        return AuthDecision(False, "missing_header")
    return AuthDecision(False, "shared_secret_mismatch")


def require_operator(x_cp_auth: str = "") -> None:
    decision = verify_shared_secret(x_cp_auth)
    if decision.allowed:
        return

    if decision.reason == "missing_secret_fail_closed":
        raise HTTPException(
            status_code=503,
            detail="Operator auth is not configured. Set CP_AUTH_SECRET environment variable.",
        )
    if decision.reason == "missing_header":
        raise HTTPException(
            status_code=401,
            detail="X-CP-Auth header required for operator endpoints.",
        )
    raise HTTPException(status_code=403, detail="Invalid operator credentials.")


def require_tenant(x_tenant_id: str = "") -> str:
    if not x_tenant_id or not x_tenant_id.strip():
        raise HTTPException(status_code=400, detail="X-Tenant-ID header required.")
    return x_tenant_id.strip()

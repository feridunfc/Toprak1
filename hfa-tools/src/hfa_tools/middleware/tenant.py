"""
hfa-tools/src/hfa_tools/middleware/tenant.py

IRONCLAD — Tenant extraction, validation, and request-scoped injection.
"""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Callable, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

logger = logging.getLogger(__name__)

_TENANT_HEADER = "X-Tenant-Id"
_RUN_ID_HEADER = "X-Run-Id"

# Tenant ID: alnum start/end, middle may contain alnum, _, ., -
_TENANT_REGEX = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]{1,98}[a-zA-Z0-9]$")


@lru_cache(maxsize=4096)
def is_valid_tenant_id(tenant_id: str) -> bool:
    return bool(_TENANT_REGEX.fullmatch(tenant_id))


def _header_get(headers, key: str):
    """
    Case-insensitive header getter.
    Works with Starlette Headers and plain dict/MagicMock-backed mappings in tests.
    """
    if headers is None:
        return None

    # Fast path
    try:
        value = headers.get(key)
        if value is not None:
            return value
    except Exception:
        pass

    # Lowercase lookup for plain dicts
    lk = str(key).lower()
    try:
        value = headers.get(lk)
        if value is not None:
            return value
    except Exception:
        pass

    # Full scan fallback
    try:
        items = headers.items()
    except Exception:
        return None

    for k, v in items:
        if str(k).lower() == lk:
            return v
    return None


class TenantError(Exception):
    """Base class for tenant middleware errors."""


class TenantMissingError(TenantError):
    """Request has no tenant identification."""


class TenantFormatError(TenantError):
    """Tenant ID / run ID does not match required format."""


class TenantMismatchError(TenantError):
    """Authenticated tenant does not match resource owner."""


@dataclass(frozen=True)
class ParsedRunId:
    tenant_id: str
    uuid_str: str
    raw: str


class TenantContext:
    __slots__ = ("tenant_id", "run_id", "source")

    def __init__(self, tenant_id: str, run_id: Optional[str], source: str) -> None:
        self.tenant_id = tenant_id
        self.run_id = run_id
        self.source = source

    def __repr__(self) -> str:
        return (
            f"TenantContext(tenant_id={self.tenant_id!r}, "
            f"run_id={self.run_id!r}, source={self.source!r})"
        )


def validate_run_id_format(run_id: str) -> tuple[str, str]:
    """
    Validate canonical run_id: run-<tenant_id>-<uuidv4>

    Returns:
        (tenant_id, normalized_uuid)
    """
    if not isinstance(run_id, str) or not run_id:
        raise TenantFormatError("run_id must be a non-empty string")

    # ✅ IRONCLAD FIX: Splitting to avoid greedy regex capturing UUID hyphens
    parts = run_id.split("-", 2)
    if len(parts) != 3 or parts[0] != "run":
        raise TenantFormatError(
            f"Invalid run_id format: {run_id!r} (expected 'run-<tenant_id>-<uuid>')"
        )

    tenant_id = parts[1]
    uuid_tail = parts[2]

    if not is_valid_tenant_id(tenant_id):
        raise TenantFormatError(f"Invalid tenant segment inside run_id: {run_id!r}")

    try:
        parsed = uuid.UUID(uuid_tail)
    except (ValueError, AttributeError) as exc:
        raise TenantFormatError(f"run_id must contain a valid UUID tail: {run_id!r}") from exc

    if parsed.int == 0:
        raise TenantFormatError("run_id UUID must not be nil UUID")

    if parsed.version != 4:
        raise TenantFormatError(f"run_id UUID must be UUIDv4, got version {parsed.version}")

    return tenant_id, str(parsed)


def parse_run_id(run_id: str) -> ParsedRunId:
    tenant_id, uuid_str = validate_run_id_format(run_id)
    return ParsedRunId(tenant_id=tenant_id, uuid_str=uuid_str, raw=run_id)


def extract_tenant_from_resource_id(resource_id: str) -> Optional[str]:
    try:
        tenant_id, _ = validate_run_id_format(resource_id)
        return tenant_id
    except TenantFormatError:
        return None


def assert_tenant_owns_resource(tenant_id: str, resource_id: str) -> None:
    extracted = extract_tenant_from_resource_id(resource_id)
    if extracted is None:
        raise TenantFormatError(f"Cannot extract tenant from resource_id={resource_id!r}")
    if extracted != tenant_id:
        raise TenantMismatchError(
            f"Tenant mismatch: authenticated={tenant_id!r} "
            f"resource_owner={extracted!r} resource_id={resource_id!r}"
        )


class TenantMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        skip_paths: Optional[set[str]] = None,
        require_run_id: bool = False,
    ) -> None:
        super().__init__(app)
        self._skip_paths = skip_paths or {
            "/health",
            "/metrics",
            "/docs",
            "/openapi.json",
        }
        self._require_run_id = require_run_id
        logger.info(
            "TenantMiddleware initialised: skip_paths=%s require_run_id=%s",
            self._skip_paths,
            require_run_id,
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        for skip in self._skip_paths:
            if request.url.path.startswith(skip):
                return await call_next(request)

        try:
            tenant_id, run_id, source = self._extract_tenant(request)
        except TenantMissingError as exc:
            logger.warning("Tenant missing: %s path=%s", exc, request.url.path)
            return Response(
                content=f"Missing tenant identification: {exc}",
                status_code=400,
            )
        except (TenantFormatError, TenantMismatchError) as exc:
            logger.warning("Tenant validation failed: %s", exc)
            return Response(
                content=f"Tenant validation failed: {exc}",
                status_code=403,
            )
        except Exception as exc:
            logger.error("Unexpected error in TenantMiddleware: %s", exc, exc_info=True)
            return Response(content="Internal server error", status_code=500)

        if self._require_run_id and not run_id:
            logger.warning("run_id required but missing: path=%s", request.url.path)
            return Response(content="X-Run-Id header required", status_code=400)

        request.state.tenant = TenantContext(
            tenant_id=tenant_id,
            run_id=run_id,
            source=source,
        )

        response = await call_next(request)
        response.headers["X-Tenant-Id"] = tenant_id
        return response

    def _extract_tenant(self, request: Request) -> tuple[str, Optional[str], str]:
        headers = getattr(request, "headers", None)
        path_params = getattr(request, "path_params", {}) or {}

        run_id: Optional[str] = _header_get(headers, _RUN_ID_HEADER)
        header_tenant = _header_get(headers, _TENANT_HEADER)

        if header_tenant:
            self._validate_format(header_tenant)
            if run_id:
                extracted, _ = validate_run_id_format(run_id)
                if extracted != header_tenant:
                    raise TenantMismatchError(
                        f"X-Tenant-Id {header_tenant!r} does not match run_id owner {extracted!r}"
                    )
            return header_tenant, run_id, "header"

        if run_id:
            extracted, _ = validate_run_id_format(run_id)
            self._validate_format(extracted)
            return extracted, run_id, "run_id_header"

        if "tenant_id" in path_params:
            tenant_id = path_params["tenant_id"]
            self._validate_format(tenant_id)

            path_run_id = path_params.get("run_id")
            if path_run_id:
                extracted, _ = validate_run_id_format(path_run_id)
                if extracted != tenant_id:
                    raise TenantMismatchError(
                        f"Path tenant_id {tenant_id!r} does not match run_id owner {extracted!r}"
                    )
            return tenant_id, path_run_id, "path_param"

        if "run_id" in path_params:
            path_run = path_params["run_id"]
            extracted, _ = validate_run_id_format(path_run)
            self._validate_format(extracted)
            return extracted, path_run, "path_run_id"

        raise TenantMissingError(
            f"No tenant identifier found in headers or path for {request.url.path!r}"
        )

    @staticmethod
    def _validate_format(tenant_id: str) -> None:
        if not is_valid_tenant_id(tenant_id):
            raise Tenant

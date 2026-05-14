"""
hfa-core/src/hfa/runtime/payload_store.py

IRONCLAD Sprint 1 — Payload Store (External Storage Tier)

TIERED STORAGE LAYER: Payload → External (S3-ready interface)

Task output payloads live outside the control plane.
Large payloads must never go through Redis — they saturate memory
and create a single point of collapse.

Storage decision (made in StateStore.store_task_output):
  payload_size ≤ INLINE_THRESHOLD_BYTES  → inline in control-plane record
  payload_size >  INLINE_THRESHOLD_BYTES → stored via PayloadStore, ref only

Tier overview:
  Backend           | When to use
  ------------------|-----------------------------
  LocalFilePayloadStore | local dev / single-node
  S3PayloadStore        | production (requires aioboto3)
  InMemoryPayloadStore  | tests / unit isolation  ← NEW (Sprint 1)

ExternalPayloadStore (new ABC, Sprint 1)
    Explicit interface contract for production external storage.
    S3PayloadStore already satisfies this contract — it is registered
    as an alias. Future backends (GCS, Azure Blob, R2) implement this.

All existing classes (PayloadStore, LocalFilePayloadStore, S3PayloadStore,
PayloadEnvelope, PayloadIntegrityError) are unchanged.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from hfa.runtime.payload_config import (
    get_payload_bucket,
    get_payload_prefix,
    get_payload_store_base_backoff_seconds,
    get_payload_store_max_retries,
    get_payload_store_timeout_seconds,
)
from hfa.runtime.payload_metrics import inc_errors


# ── Data types (unchanged) ────────────────────────────────────────────────────

@dataclass(frozen=True)
class PayloadEnvelope:
    """Metadata returned after storing a payload externally."""
    payload_ref: str
    payload_size: int
    checksum: str
    payload_type: str = "binary"


class PayloadIntegrityError(RuntimeError):
    """Raised when a retrieved payload fails its checksum verification."""
    pass


# ── PayloadStore — base class (unchanged) ─────────────────────────────────────

class PayloadStore:
    """
    Base class for payload storage backends.

    Callers interact with this interface. Subclass to add a new backend.
    """

    async def put(self, data: bytes) -> PayloadEnvelope:
        raise NotImplementedError

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        raise NotImplementedError


# ── ExternalPayloadStore — explicit ABC for production backends ───────────────

class ExternalPayloadStore(PayloadStore, ABC):
    """
    Abstract interface for production external payload storage.

    Semantics:
      put(data)       — durably store bytes, return a stable reference
      get(ref)        — retrieve bytes by reference, verify checksum if given
      delete(ref)     — remove payload (for TTL / archival pipelines)
      exists(ref)     — cheaply check presence without fetching data

    Guarantees expected from implementations:
      * put() is idempotent for the same data (content-addressed preferred)
      * get() with expected_checksum raises PayloadIntegrityError on mismatch
      * delete() is best-effort — implementations may no-op if unsupported
      * All methods are async and safe to call concurrently

    Current implementations:
      S3PayloadStore          — production (aioboto3, optional dep)
      LocalFilePayloadStore   — dev / single-node (no extra deps)
      InMemoryPayloadStore    — tests / unit isolation (no extra deps)
    """

    @abstractmethod
    async def put(self, data: bytes) -> PayloadEnvelope:
        raise NotImplementedError

    @abstractmethod
    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        raise NotImplementedError

    async def delete(self, ref: str) -> None:
        """
        Remove a stored payload.

        Default: no-op. Override in backends that support deletion.
        Callers must tolerate this being a no-op.
        """
        pass

    async def exists(self, ref: str) -> bool:
        """
        Check whether a payload ref is accessible.

        Default: attempt get() and return True on success.
        Override for cheaper HEAD-style checks in production backends.
        """
        try:
            await self.get(ref)
            return True
        except Exception:
            return False


# ── LocalFilePayloadStore (unchanged) ─────────────────────────────────────────

class LocalFilePayloadStore(ExternalPayloadStore):
    """File-system payload store. For local dev and single-node deployments."""

    def __init__(self, root_dir: str | None = None) -> None:
        self._root = Path(
            root_dir
            or os.getenv("PAYLOAD_LOCAL_DIR")
            or tempfile.gettempdir() + "/ironclad_payloads"
        )
        self._root.mkdir(parents=True, exist_ok=True)

    async def put(self, data: bytes) -> PayloadEnvelope:
        checksum = hashlib.sha256(data).hexdigest()
        key = f"{uuid.uuid4().hex}.bin"
        path = self._root / key
        path.write_bytes(data)
        return PayloadEnvelope(
            payload_ref=f"file://{path.as_posix()}",
            payload_size=len(data),
            checksum=checksum,
        )

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        if not ref.startswith("file://"):
            raise ValueError(f"Unsupported local payload ref: {ref}")
        path = Path(ref[len("file://"):])
        data = path.read_bytes()
        if expected_checksum is not None:
            checksum = hashlib.sha256(data).hexdigest()
            if checksum != expected_checksum:
                raise PayloadIntegrityError(f"Checksum mismatch for {ref}")
        return data

    async def delete(self, ref: str) -> None:
        if not ref.startswith("file://"):
            return
        path = Path(ref[len("file://"):])
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    async def exists(self, ref: str) -> bool:
        if not ref.startswith("file://"):
            return False
        return Path(ref[len("file://"):]).exists()


# ── InMemoryPayloadStore — tests / unit isolation (new, Sprint 1) ─────────────

class InMemoryPayloadStore(ExternalPayloadStore):
    """
    In-memory payload store. For tests and unit isolation.

    No external dependencies. Data is lost when the process exits.
    Safe to use in concurrent tests — each instance has its own store.
    """

    def __init__(self) -> None:
        self._store: Dict[str, bytes] = {}

    async def put(self, data: bytes) -> PayloadEnvelope:
        checksum = hashlib.sha256(data).hexdigest()
        ref = f"mem://{uuid.uuid4().hex}"
        self._store[ref] = data
        return PayloadEnvelope(
            payload_ref=ref,
            payload_size=len(data),
            checksum=checksum,
        )

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        if ref not in self._store:
            raise KeyError(f"Payload not found: {ref}")
        data = self._store[ref]
        if expected_checksum is not None:
            checksum = hashlib.sha256(data).hexdigest()
            if checksum != expected_checksum:
                raise PayloadIntegrityError(f"Checksum mismatch for {ref}")
        return data

    async def delete(self, ref: str) -> None:
        self._store.pop(ref, None)

    async def exists(self, ref: str) -> bool:
        return ref in self._store

    def clear(self) -> None:
        """Reset store. Useful in test teardown."""
        self._store.clear()


# ── S3PayloadStore (unchanged) ────────────────────────────────────────────────

class S3PayloadStore(ExternalPayloadStore):
    """
    S3-backed payload store. Production backend.
    Requires: aioboto3 (optional dependency, not in base requirements).
    """

    def __init__(self, bucket: str | None = None, prefix: str | None = None) -> None:
        self._bucket = bucket or get_payload_bucket()
        self._prefix = prefix or get_payload_prefix()

    async def _with_retries(self, op):
        retries = get_payload_store_max_retries()
        base_backoff = get_payload_store_base_backoff_seconds()
        timeout = get_payload_store_timeout_seconds()
        last_exc = None
        for attempt in range(retries):
            try:
                return await asyncio.wait_for(op(), timeout=timeout)
            except Exception as exc:
                last_exc = exc
                inc_errors()
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(base_backoff * (2 ** attempt))
        raise last_exc  # type: ignore[misc]

    async def put(self, data: bytes) -> PayloadEnvelope:
        checksum = hashlib.sha256(data).hexdigest()
        key = f"{self._prefix}/{uuid.uuid4().hex}.bin"

        async def _op():
            import aioboto3  # type: ignore[import]
            session = aioboto3.Session()
            async with session.client("s3") as s3:
                await s3.put_object(Bucket=self._bucket, Key=key, Body=data)
            return None

        await self._with_retries(_op)
        return PayloadEnvelope(
            payload_ref=f"s3://{self._bucket}/{key}",
            payload_size=len(data),
            checksum=checksum,
        )

    async def get(self, ref: str, *, expected_checksum: Optional[str] = None) -> bytes:
        if not ref.startswith("s3://"):
            raise ValueError(f"Unsupported s3 payload ref: {ref}")
        bucket_and_key = ref[len("s3://"):]
        bucket, key = bucket_and_key.split("/", 1)

        async def _op():
            import aioboto3  # type: ignore[import]
            session = aioboto3.Session()
            async with session.client("s3") as s3:
                response = await s3.get_object(Bucket=bucket, Key=key)
                return await response["Body"].read()

        data = await self._with_retries(_op)
        if expected_checksum is not None:
            checksum = hashlib.sha256(data).hexdigest()
            if checksum != expected_checksum:
                raise PayloadIntegrityError(f"Checksum mismatch for {ref}")
        return data

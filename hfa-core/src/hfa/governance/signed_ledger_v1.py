"""
hfa-core/src/hfa/governance/signed_ledger_v1.py
IRONCLAD — Tamper-evident, Ed25519-signed append-only ledger.

Design
------
* KeyProvider ABC decouples key storage (env-var, Vault, KMS)
  from ledger logic — swap implementations without touching ledger code.
* Every entry is: JSON payload + Ed25519 signature over
  sha256(prev_hash || sequence || payload_bytes).
  Chain integrity → any tampering breaks signature of all subsequent entries.
* Verification is stateless: give it the public key and the entry list.
* Storage backend is injectable (InMemoryLedgerStore in tests;
  Redis/Postgres in production — Sprint 4 adds RedisLedgerStore).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# KeyProvider ABC
# ---------------------------------------------------------------------------


class KeyProvider(ABC):
    """
    Abstract factory for Ed25519 signing/verification.
    Implementations can load keys from env-vars, HashiCorp Vault,
    AWS KMS, GCP Cloud KMS, etc.
    """

    @abstractmethod
    async def sign(self, data: bytes) -> bytes:
        """
        Sign data with the private key.

        Args:
            data: Raw bytes to sign.

        Returns:
            Raw signature bytes (64 bytes for Ed25519).

        Raises:
            KeyProviderError: On signing failure.
        """

    @abstractmethod
    async def verify(self, data: bytes, signature: bytes) -> bool:
        """
        Verify a signature against data using the public key.

        Args:
            data:      Original data bytes.
            signature: Signature bytes to verify.

        Returns:
            True if valid, False otherwise.

        Raises:
            KeyProviderError: On verification infrastructure failure.
        """

    @property
    @abstractmethod
    def key_id(self) -> str:
        """Logical key identifier for audit purposes (e.g. 'prod-2025-03')."""


class KeyProviderError(Exception):
    """Raised when key operations fail (key not found, HSM error, etc.)."""


# ---------------------------------------------------------------------------
# Ed25519EnvKeyProvider  — loads key from env-var or PEM bytes
# ---------------------------------------------------------------------------


class Ed25519EnvKeyProvider(KeyProvider):
    """
    KeyProvider that loads Ed25519 keys from base64-encoded env-vars
    or from PEM bytes passed directly.

    Expected env-vars (set by genesis.py generate-ledger-key --env-snippet):
      HFA_LEDGER_PRIVATE_KEY_B64  — base64(PKCS8 PEM)
      HFA_LEDGER_PUBLIC_KEY_B64   — base64(SubjectPublicKeyInfo PEM)
      HFA_LEDGER_KEY_ID           — logical key identifier

    Args:
        private_pem: PKCS8 PEM bytes of the private key (optional).
        public_pem:  SubjectPublicKeyInfo PEM bytes (optional).
        key_id:      Logical key identifier.

    If pem args are None, the provider reads from env-vars at first use
    (lazy loading for container/K8s secret mount compatibility).

    Raises:
        KeyProviderError: If keys cannot be loaded.
    """

    def __init__(
        self,
        private_pem: Optional[bytes] = None,
        public_pem: Optional[bytes] = None,
        key_id: str = "default",
    ) -> None:
        self._private_pem = private_pem
        self._public_pem = public_pem
        self._key_id = key_id
        self._private_key = None
        self._public_key = None

    @property
    def key_id(self) -> str:
        return self._key_id

    async def sign(self, data: bytes) -> bytes:
        """Sign data. Lazy-loads private key on first call."""
        loop = asyncio.get_running_loop()
        private_key = await loop.run_in_executor(None, self._load_private_key)
        try:
            sig: bytes = await loop.run_in_executor(None, private_key.sign, data)
            return sig
        except Exception as exc:
            logger.error("Ed25519 sign failed: %s", exc, exc_info=True)
            raise KeyProviderError("Signing failed") from exc

    async def verify(self, data: bytes, signature: bytes) -> bool:
        """Verify signature. Lazy-loads public key on first call."""
        loop = asyncio.get_running_loop()
        public_key = await loop.run_in_executor(None, self._load_public_key)
        try:
            await loop.run_in_executor(None, public_key.verify, signature, data)
            return True
        except Exception:
            # cryptography raises InvalidSignature — treat as False, not error
            return False

    # ------------------------------------------------------------------
    # Private helpers (run in thread pool — no blocking in async context)
    # ------------------------------------------------------------------

    def _load_private_key(self):
        """Load private key, using cache after first load."""
        if self._private_key is not None:
            return self._private_key

        try:
            from cryptography.hazmat.primitives.serialization import (
                load_pem_private_key,
            )
        except ImportError as exc:
            raise KeyProviderError(
                "cryptography package required: pip install cryptography>=42.0"
            ) from exc

        pem = self._private_pem or self._read_b64_env("HFA_LEDGER_PRIVATE_KEY_B64")
        try:
            key = load_pem_private_key(pem, password=None)
            self._private_key = key

            # Infer key_id from settings if not explicitly set
            if self._key_id == "default":
                from hfa.core.config import settings

                env_id = getattr(settings, "HFA_LEDGER_KEY_ID", None)
                if env_id:
                    self._key_id = env_id

            return key
        except Exception as exc:
            logger.error("Failed to load private key: %s", exc, exc_info=True)
            raise KeyProviderError("Invalid private key PEM") from exc

    def _load_public_key(self):
        """Load public key, using cache after first load."""
        if self._public_key is not None:
            return self._public_key

        try:
            from cryptography.hazmat.primitives.serialization import load_pem_public_key
        except ImportError as exc:
            raise KeyProviderError(
                "cryptography package required: pip install cryptography>=42.0"
            ) from exc

        # Derive public key from private if public PEM not provided
        if self._public_pem is None and self._private_key is not None:
            self._public_key = self._private_key.public_key()
            return self._public_key

        pem = self._public_pem or self._read_b64_env("HFA_LEDGER_PUBLIC_KEY_B64")
        try:
            key = load_pem_public_key(pem)
            self._public_key = key
            return key
        except Exception as exc:
            logger.error("Failed to load public key: %s", exc, exc_info=True)
            raise KeyProviderError("Invalid public key PEM") from exc

    @staticmethod
    def _read_b64_env(var_name: str) -> bytes:
        """Read a base64-encoded value from the application config (env-backed)."""
        import base64
        from hfa.core.config import settings

        # Map well-known ledger var names to settings attributes
        _setting_map = {
            "HFA_LEDGER_PRIVATE_KEY_B64": "HFA_LEDGER_PRIVATE_KEY_B64",
            "HFA_LEDGER_PUBLIC_KEY_B64": "HFA_LEDGER_PUBLIC_KEY_B64",
        }
        attr = _setting_map.get(var_name, var_name)
        val = getattr(settings, attr, None)
        if not val:
            raise KeyProviderError(
                f"Config setting {attr!r} not set "
                f"(set {var_name} env-var or use genesis.py generate-ledger-key)"
            )
        try:
            return base64.b64decode(val)
        except Exception as exc:
            raise KeyProviderError(
                f"Failed to base64-decode config setting {attr!r}"
            ) from exc


# ---------------------------------------------------------------------------
# LedgerEntry — immutable signed record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LedgerEntry:
    """
    A single, signed entry in the append-only ledger.

    Fields
    ------
    entry_id:    UUID v4 string.
    sequence:    Monotonically increasing integer (1-based).
    tenant_id:   Owning tenant.
    run_id:      Associated run.
    event_type:  Arbitrary event category (e.g. "llm_call", "tool_use").
    payload:     Arbitrary event data (must be JSON-serialisable).
    timestamp:   Unix epoch float (UTC).
    prev_hash:   SHA-256 hex of the previous entry's canonical bytes.
                 Empty string for the genesis (first) entry.
    signature:   Hex-encoded Ed25519 signature over canonical bytes.
    key_id:      Logical key identifier (for key rotation audit).
    """

    entry_id: str
    sequence: int
    tenant_id: str
    run_id: str
    event_type: str
    payload: dict
    timestamp: float
    prev_hash: str
    signature: str
    key_id: str

    def canonical_bytes(self) -> bytes:
        """
        Deterministic byte representation used for signing and chaining.
        prev_hash + sequence + stable JSON payload.
        """
        body = json.dumps(
            {
                "entry_id": self.entry_id,
                "sequence": self.sequence,
                "tenant_id": self.tenant_id,
                "run_id": self.run_id,
                "event_type": self.event_type,
                "payload": self.payload,
                "timestamp": self.timestamp,
                "prev_hash": self.prev_hash,
                "key_id": self.key_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return body

    def content_hash(self) -> str:
        """SHA-256 hex of canonical bytes (used as prev_hash for next entry)."""
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def to_dict(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "sequence": self.sequence,
            "tenant_id": self.tenant_id,
            "run_id": self.run_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "prev_hash": self.prev_hash,
            "signature": self.signature,
            "key_id": self.key_id,
        }


# ---------------------------------------------------------------------------
# LedgerStore ABC + InMemory implementation
# ---------------------------------------------------------------------------


class LedgerStore(ABC):
    """Pluggable storage backend for LedgerEntry objects."""

    @abstractmethod
    async def append(self, entry: LedgerEntry) -> None:
        """Atomically append an entry. Must be idempotent on entry_id."""

    @abstractmethod
    async def get_last(self, tenant_id: str, run_id: str) -> Optional[LedgerEntry]:
        """Return the most recent entry for the run, or None."""

    @abstractmethod
    async def get_all(self, tenant_id: str, run_id: str) -> list[LedgerEntry]:
        """Return all entries for the run in sequence order."""


class InMemoryLedgerStore(LedgerStore):
    """Thread-safe in-memory store for tests and development."""

    def __init__(self) -> None:
        self._store: dict[str, list[LedgerEntry]] = {}
        self._lock = asyncio.Lock()

    async def append(self, entry: LedgerEntry) -> None:
        key = f"{entry.tenant_id}:{entry.run_id}"
        async with self._lock:
            entries = self._store.setdefault(key, [])
            # Idempotency: skip duplicates by entry_id
            if any(e.entry_id == entry.entry_id for e in entries):
                logger.debug(
                    "LedgerStore: duplicate entry_id=%s ignored", entry.entry_id
                )
                return
            entries.append(entry)

    async def get_last(self, tenant_id: str, run_id: str) -> Optional[LedgerEntry]:
        key = f"{tenant_id}:{run_id}"
        entries = self._store.get(key, [])
        return entries[-1] if entries else None

    async def get_all(self, tenant_id: str, run_id: str) -> list[LedgerEntry]:
        key = f"{tenant_id}:{run_id}"
        return list(self._store.get(key, []))


# ---------------------------------------------------------------------------
# SignedLedger — the main API
# ---------------------------------------------------------------------------


class LedgerIntegrityError(Exception):
    """Raised when chain verification fails (tamper detected)."""


class SignedLedger:
    """
    Ed25519-signed, append-only ledger with chain integrity verification.

    Usage
    -----
    provider = Ed25519EnvKeyProvider(key_id="prod-2025-03")
    ledger   = SignedLedger(key_provider=provider)

    await ledger.append(
        tenant_id="acme_corp",
        run_id="run-acme_corp-550e8400",
        event_type="llm_call",
        payload={"model": "gpt-4o", "tokens": 312, "cost_cents": 31},
    )

    ok  = await ledger.verify_chain("acme_corp", "run-acme_corp-550e8400")
    await ledger.close()   # drain in-flight writes before shutdown

    Args:
        key_provider: KeyProvider implementation.
        store:        LedgerStore implementation (default: InMemoryLedgerStore).

    Raises:
        LedgerIntegrityError: When chain verification detects tampering.
        KeyProviderError:     On signing/verification failure.
    """

    def __init__(
        self,
        key_provider: KeyProvider,
        store: Optional[LedgerStore] = None,
    ) -> None:
        self._key = key_provider
        self._store = store or InMemoryLedgerStore()
        self._in_flight: set[asyncio.Task] = set()  # track pending writes
        logger.info(
            "SignedLedger initialised: key_id=%s store=%s",
            key_provider.key_id,
            type(self._store).__name__,
        )

    async def append(
        self,
        tenant_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> LedgerEntry:
        """
        Sign and append a new entry to the ledger.

        The prev_hash chains this entry to the previous one,
        making the entire log tamper-evident.

        Args:
            tenant_id:  Owning tenant.
            run_id:     Associated run.
            event_type: Category string (e.g. "llm_call", "tool_use", "budget_debit").
            payload:    Arbitrary JSON-serialisable event data.

        Returns:
            The signed LedgerEntry that was appended.

        Raises:
            KeyProviderError: On signing failure.
        """
        last = await self._store.get_last(tenant_id, run_id)
        sequence = (last.sequence + 1) if last else 1
        prev_hash = last.content_hash() if last else ""

        # Build unsigned entry first (signature field placeholder "")
        entry_id = str(uuid.uuid4())
        unsigned = LedgerEntry(
            entry_id=entry_id,
            sequence=sequence,
            tenant_id=tenant_id,
            run_id=run_id,
            event_type=event_type,
            payload=payload,
            timestamp=time.time(),
            prev_hash=prev_hash,
            signature="",
            key_id=self._key.key_id,
        )

        sig_bytes = await self._key.sign(unsigned.canonical_bytes())
        signature_hex = sig_bytes.hex()

        # Create final immutable entry with real signature
        signed = LedgerEntry(
            entry_id=entry_id,
            sequence=sequence,
            tenant_id=tenant_id,
            run_id=run_id,
            event_type=event_type,
            payload=payload,
            timestamp=unsigned.timestamp,
            prev_hash=prev_hash,
            signature=signature_hex,
            key_id=self._key.key_id,
        )

        await self._store.append(signed)
        logger.info(
            "Ledger append: tenant=%s run=%s seq=%d event=%s",
            tenant_id,
            run_id,
            sequence,
            event_type,
        )
        return signed

    async def verify_chain(self, tenant_id: str, run_id: str) -> bool:
        """
        Verify the integrity of the entire entry chain for a run.

        Checks:
          1. Each entry's signature is valid.
          2. Each entry's prev_hash matches the content_hash of the prior entry.
          3. Sequence numbers are consecutive starting at 1.

        Args:
            tenant_id: Tenant identifier.
            run_id:    Run identifier.

        Returns:
            True if chain is intact.

        Raises:
            LedgerIntegrityError: If any check fails (tamper detected).
            KeyProviderError:     On verification infrastructure failure.
        """
        entries = await self._store.get_all(tenant_id, run_id)
        if not entries:
            logger.info("verify_chain: empty ledger for %s/%s", tenant_id, run_id)
            return True

        for i, entry in enumerate(entries):
            # 1. Sequence check
            expected_seq = i + 1
            if entry.sequence != expected_seq:
                raise LedgerIntegrityError(
                    f"Sequence mismatch at position {i}: "
                    f"expected={expected_seq} got={entry.sequence}"
                )

            # 2. Chain hash check
            if i == 0:
                if entry.prev_hash != "":
                    raise LedgerIntegrityError(
                        "Genesis entry must have empty prev_hash"
                    )
            else:
                expected_prev = entries[i - 1].content_hash()
                if entry.prev_hash != expected_prev:
                    raise LedgerIntegrityError(
                        f"Chain broken at sequence {entry.sequence}: prev_hash mismatch"
                    )

            # 3. Signature check — verify against unsigned canonical bytes
            unsigned = LedgerEntry(
                entry_id=entry.entry_id,
                sequence=entry.sequence,
                tenant_id=entry.tenant_id,
                run_id=entry.run_id,
                event_type=entry.event_type,
                payload=entry.payload,
                timestamp=entry.timestamp,
                prev_hash=entry.prev_hash,
                signature="",
                key_id=entry.key_id,
            )
            sig_bytes = bytes.fromhex(entry.signature)
            valid = await self._key.verify(unsigned.canonical_bytes(), sig_bytes)
            if not valid:
                raise LedgerIntegrityError(
                    f"Invalid signature at sequence {entry.sequence} "
                    f"(entry_id={entry.entry_id})"
                )

        logger.info(
            "verify_chain OK: tenant=%s run=%s entries=%d",
            tenant_id,
            run_id,
            len(entries),
        )
        return True

    async def get_entries(
        self,
        tenant_id: str,
        run_id: str,
    ) -> list[LedgerEntry]:
        """Return all entries for a run in sequence order."""
        return await self._store.get_all(tenant_id, run_id)

    async def close(self) -> None:
        """
        Drain all in-flight append tasks before shutdown.

        Waits for every pending write to complete (or fail gracefully).
        Must be called from the application lifespan shutdown hook to
        guarantee no signed entries are lost on orderly shutdown.

        Usage
        -----
            # FastAPI lifespan
            @asynccontextmanager
            async def lifespan(app):
                yield
                await app.state.ledger.close()
        """
        if not self._in_flight:
            logger.info("SignedLedger.close(): no in-flight writes, done")
            return

        count = len(self._in_flight)
        logger.info("SignedLedger.close(): draining %d in-flight write(s)…", count)
        results = await asyncio.gather(*self._in_flight, return_exceptions=True)

        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            logger.error(
                "SignedLedger.close(): %d write(s) failed during drain: %s",
                len(errors),
                errors,
            )
        else:
            logger.info("SignedLedger.close(): all %d write(s) drained OK", count)

        self._in_flight.clear()

    # ------------------------------------------------------------------
    # Async context manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "SignedLedger":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

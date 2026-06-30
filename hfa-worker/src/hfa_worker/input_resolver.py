from __future__ import annotations

import inspect
import json
import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

from hfa.runtime.lineage_store import LineageStore
from hfa.runtime.payload_store import PayloadStore


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


class MissingParentOutputError(KeyError):
    """Raised when a required parent output cannot be resolved."""


@dataclass(frozen=True, eq=False)
class ResolvedOutput:
    data: Any
    payload_ref: str | None
    checksum: str | None

    def __eq__(self, other: object) -> bool:
        """Backward-compatible comparison for legacy callers expecting raw payload bytes."""
        if isinstance(other, (bytes, bytearray)):
            return self.data == bytes(other)
        if isinstance(other, ResolvedOutput):
            return (
                self.data == other.data
                and self.payload_ref == other.payload_ref
                and self.checksum == other.checksum
            )
        return False


@dataclass
class ResolveResult:
    hydrated: dict
    lineage_record: dict | None = None
    referenced_task_ids: list = field(default_factory=list)


class InputResolver:
    def __init__(
        self,
        redis_client,
        payload_store: PayloadStore | None = None,
        lineage_store: LineageStore | None = None,
    ) -> None:
        self._redis = redis_client
        self._payload_store = payload_store
        self._lineage_store = lineage_store
        self._referenced_ids: list[str] = []
        self._parent_records: dict[str, dict[str, Any]] = {}

    async def resolve(self, template) -> ResolveResult:
        self._referenced_ids = []
        self._parent_records = {}
        self._output_cache: dict[str, Any] = {}
        await self._prefetch_outputs(template)
        hydrated = await self._resolve_value(template)
        referenced = list(dict.fromkeys(self._referenced_ids))
        parent_sha_material = "|".join(
            self._parent_records[tid]["sha256"]
            for tid in sorted(self._parent_records)
        )
        lineage = {
            "resolved_keys": list(template.keys()) if isinstance(template, dict) else [],
            "parent_task_ids": referenced,
            "parent_count": len(referenced),
            "parents": dict(self._parent_records),
            "combined_sha256": hashlib.sha256(parent_sha_material.encode("utf-8")).hexdigest(),
        }
        return ResolveResult(
            hydrated=hydrated,
            lineage_record=lineage,
            referenced_task_ids=referenced,
        )

    async def persist_lineage(self, *, task_id: str, lineage_record: dict | None, ttl_seconds: int = 3600) -> None:
        if lineage_record is None:
            return
        from hfa.dag.schema import DagRedisKey
        key = DagRedisKey.task_lineage(task_id)
        await self._redis.set(key, json.dumps(lineage_record), ex=ttl_seconds)

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _resolve_value(self, value):
        if isinstance(value, dict):
            if "__merge__" in value:
                return await self._resolve_merge(value)
            return {k: await self._resolve_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [await self._resolve_value(item) for item in value]
        if isinstance(value, str):
            return await self._resolve_string(value)
        return value

    def _collect_task_ids(self, value) -> list[str]:
        pattern = re.compile(r'\$\{([^}]+)\.output\}')
        found: list[str] = []

        def walk(item) -> None:
            if isinstance(item, dict):
                for nested in item.values():
                    walk(nested)
                return
            if isinstance(item, list):
                for nested in item:
                    walk(nested)
                return
            if isinstance(item, str):
                for match in pattern.finditer(item):
                    found.append(match.group(1))

        walk(value)
        return list(dict.fromkeys(found))

    async def _prefetch_outputs(self, template) -> None:
        task_ids = self._collect_task_ids(template)
        if not task_ids or not hasattr(self._redis, "mget"):
            return

        keys = [f"hfa:dag:task:{task_id}:output" for task_id in task_ids]
        try:
            values = await _maybe_await(self._redis.mget(keys))
        except Exception:
            return

        if not isinstance(values, (list, tuple)):
            return

        for task_id, raw in zip(task_ids, values):
            self._output_cache[task_id] = raw

    async def _resolve_string(self, s: str):
        """Resolve ${task_id.output} placeholders. JSON strings are deserialized to objects."""
        pattern = re.compile(r'\$\{([^}]+)\.output\}')
        matches = list(pattern.finditer(s))
        if not matches:
            return s

        # Single full-string placeholder → return deserialized value
        if len(matches) == 1 and matches[0].start() == 0 and matches[0].end() == len(s):
            task_id = matches[0].group(1)
            self._referenced_ids.append(task_id)
            raw = await self._fetch_output(task_id)
            try:
                return json.loads(raw)
            except Exception:
                return raw

        # Inline placeholder inside string → string substitution
        # Inline placeholder inside string: preserve encounter order.
        parts: list[str] = []
        last = 0
        for m in matches:
            task_id = m.group(1)
            self._referenced_ids.append(task_id)
            raw = await self._fetch_output(task_id)
            parts.append(s[last:m.start()])
            parts.append(str(raw))
            last = m.end()
        parts.append(s[last:])
        return "".join(parts)

    async def _resolve_merge(self, directive: dict):
        merge_spec = directive["__merge__"]
        mode = directive.get("mode", "list")

        async def _fetch(task_id: str):
            self._referenced_ids.append(task_id)
            raw = await self._fetch_output(task_id)
            try:
                return json.loads(raw)
            except Exception:
                return raw

        if mode == "list" and isinstance(merge_spec, list):
            return [await _fetch(tid) for tid in merge_spec]
        if mode == "dict" and isinstance(merge_spec, dict):
            return {alias: await _fetch(tid) for alias, tid in merge_spec.items()}
        return merge_spec

    async def _fetch_output(self, task_id: str) -> str:
        sentinel = object()
        raw = getattr(self, "_output_cache", {}).get(task_id, sentinel)

        if raw is sentinel:
            raw = await _maybe_await(self._redis.get(f"hfa:dag:task:{task_id}:output"))

        if raw is None:
            raise MissingParentOutputError(f"Missing output for task_id={task_id}")

        raw_text = raw.decode() if isinstance(raw, bytes) else str(raw)

        if task_id not in self._parent_records:
            try:
                decoded = json.loads(raw_text)
            except Exception:
                decoded = raw_text

            self._parent_records[task_id] = {
                "raw": raw_text,
                "decoded": decoded,
                "sha256": hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
            }

        return raw_text

    # ── Legacy API ──────────────────────────────────────────────────────────

    async def resolve_input(
        self,
        task_id: str,
        *,
        run_id: str = "",
        consumer_task_id: str = "",
        consumer_worker_id: str = "",
        consumer_attempt: int = 0,
    ) -> ResolvedOutput:
        raw = await _maybe_await(self._redis.get(f"hfa:task:{task_id}:output"))
        if raw is None:
            raise MissingParentOutputError(f"Missing output for task_id={task_id}")
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        record = json.loads(raw)
        mode = record["payload_mode"]
        if mode == "inline":
            resolved = ResolvedOutput(
                data=record["payload_inline"],
                payload_ref=record.get("payload_ref"),
                checksum=record.get("checksum"),
            )
        elif mode == "ref":
            if self._payload_store is None:
                raise RuntimeError("Payload ref found but no payload_store configured")
            data = await self._payload_store.get(
                record["payload_ref"],
                expected_checksum=record.get("checksum"),
            )
            if record.get("payload_type") == "text":
                data = data.decode("utf-8")
            resolved = ResolvedOutput(
                data=data,
                payload_ref=record.get("payload_ref"),
                checksum=record.get("checksum"),
            )
        else:
            raise ValueError(f"Unknown payload_mode={mode}")

        if (self._lineage_store is not None and run_id and consumer_task_id
                and consumer_worker_id and consumer_attempt):
            await self._lineage_store.record_consumed_input(
                run_id=run_id,
                parent_task_id=task_id,
                consumer_task_id=consumer_task_id,
                consumed_output_ref=resolved.payload_ref,
                checksum=resolved.checksum or "",
                consumer_worker_id=consumer_worker_id,
                consumer_attempt=consumer_attempt,
            )
            await self._lineage_store.record_lineage_edge(
                run_id=run_id,
                parent_task_id=task_id,
                child_task_id=consumer_task_id,
            )
        return resolved

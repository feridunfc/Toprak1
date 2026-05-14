from __future__ import annotations

import inspect
import json
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


@dataclass(frozen=True)
class ResolvedOutput:
    data: Any
    payload_ref: str | None
    checksum: str | None


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

    async def resolve(self, template) -> ResolveResult:
        self._referenced_ids = []
        hydrated = await self._resolve_value(template)
        referenced = list(set(self._referenced_ids))
        lineage = {
            "resolved_keys": list(template.keys()) if isinstance(template, dict) else [],
            "parent_task_ids": referenced,
            "parent_count": len(referenced),
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
        result = s
        for m in reversed(matches):
            task_id = m.group(1)
            self._referenced_ids.append(task_id)
            raw = await self._fetch_output(task_id)
            # Keep as string for inline interpolation
            result = result[:m.start()] + raw + result[m.end():]
        return result

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
        raw = await self._redis.get(f"hfa:dag:task:{task_id}:output")
        if raw is None:
            raise MissingParentOutputError(f"Missing output for task_id={task_id}")
        return raw.decode() if isinstance(raw, bytes) else raw

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

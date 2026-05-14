"""
tests/fakestore.py
Minimal async in-memory Redis stub for Sprint 12 unit tests.
Implements only the Redis commands used by StateStore, WorkerRegistry,
RecoveryService, and DrainManager.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple


class FakeRedis:
    """Thread-safe async in-memory Redis stub."""

    def __init__(self):
        self._strings: Dict[str, Tuple[str, Optional[float]]] = {}  # key -> (value, expire_at)
        self._hashes: Dict[str, Dict[str, str]] = {}
        self._sets: Dict[str, set] = {}
        self._zsets: Dict[str, Dict[str, float]] = {}  # key -> {member: score}
        self._streams: Dict[str, List[Tuple[str, Dict[bytes, bytes]]]] = {}
        self._stream_seq: Dict[str, int] = {}
        self._groups: Dict[str, Dict[str, Any]] = {}  # stream -> {group -> {consumer -> [ids]}}

    # ------------------------------------------------------------------ #
    # Expiry helpers
    # ------------------------------------------------------------------ #

    def _is_expired(self, key: str) -> bool:
        if key in self._strings:
            _, exp = self._strings[key]
            if exp is not None and time.time() > exp:
                del self._strings[key]
                return True
        return False

    # ------------------------------------------------------------------ #
    # String commands
    # ------------------------------------------------------------------ #

    async def set(self, key: str, value: Any, ex: int = None, nx: bool = False) -> Any:
        if nx and not self._is_expired(key) and key in self._strings:
            return None
        exp = time.time() + ex if ex else None
        self._strings[key] = (str(value), exp)
        return True

    async def get(self, key: str) -> Optional[bytes]:
        if self._is_expired(key):
            return None
        entry = self._strings.get(key)
        if entry is None:
            return None
        val, _ = entry
        return val.encode() if val is not None else None

    async def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            k_str = k.decode() if isinstance(k, bytes) else k
            if k_str in self._strings:
                del self._strings[k_str]
                count += 1
            if k_str in self._hashes:
                del self._hashes[k_str]
                count += 1
        return count

    async def exists(self, key: str) -> int:
        if self._is_expired(key):
            return 0
        return 1 if (key in self._strings or key in self._hashes) else 0

    async def expire(self, key: str, seconds: int) -> int:
        if key in self._strings:
            val, _ = self._strings[key]
            self._strings[key] = (val, time.time() + seconds)
            return 1
        if key in self._hashes:
            # Hashes don't expire in this stub but we track it
            return 1
        return 0

    async def ttl(self, key: str) -> int:
        entry = self._strings.get(key)
        if entry is None:
            return -2
        _, exp = entry
        if exp is None:
            return -1
        remaining = int(exp - time.time())
        return remaining if remaining > 0 else -2

    async def ping(self) -> bool:
        return True

    # ------------------------------------------------------------------ #
    # Hash commands
    # ------------------------------------------------------------------ #

    async def hset(
        self, key: str, field: str = None, value: str = None, mapping: Dict = None
    ) -> int:
        if key not in self._hashes:
            self._hashes[key] = {}
        count = 0
        if mapping:
            for k, v in mapping.items():
                k_str = k.decode() if isinstance(k, bytes) else str(k)
                v_str = v.decode() if isinstance(v, bytes) else str(v)
                self._hashes[key][k_str] = v_str
                count += 1
        if field is not None:
            f = field.decode() if isinstance(field, bytes) else str(field)
            v = value.decode() if isinstance(value, bytes) else str(value)
            self._hashes[key][f] = v
            count += 1
        return count

    async def hget(self, key: str, field: str) -> Optional[bytes]:
        h = self._hashes.get(key, {})
        f = field.decode() if isinstance(field, bytes) else str(field)
        val = h.get(f)
        return val.encode() if val is not None else None

    async def hgetall(self, key: str) -> Dict[bytes, bytes]:
        k = key.decode() if isinstance(key, bytes) else key
        h = self._hashes.get(k, {})
        return {kk.encode(): vv.encode() for kk, vv in h.items()}

    # ------------------------------------------------------------------ #
    # Set commands
    # ------------------------------------------------------------------ #

    async def sadd(self, key: str, *members) -> int:
        if key not in self._sets:
            self._sets[key] = set()
        added = 0
        for m in members:
            m_str = m.decode() if isinstance(m, bytes) else str(m)
            if m_str not in self._sets[key]:
                self._sets[key].add(m_str)
                added += 1
        return added

    async def smembers(self, key: str) -> set:
        return {m.encode() for m in self._sets.get(key, set())}

    # ------------------------------------------------------------------ #
    # Sorted set commands
    # ------------------------------------------------------------------ #

    async def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        if key not in self._zsets:
            self._zsets[key] = {}
        for m, s in mapping.items():
            self._zsets[key][m] = float(s)
        return len(mapping)

    async def zrem(self, key: str, *members) -> int:
        z = self._zsets.get(key, {})
        count = 0
        for m in members:
            m_str = m.decode() if isinstance(m, bytes) else str(m)
            if m_str in z:
                del z[m_str]
                count += 1
        return count

    async def zrangebyscore(self, key: str, min_score, max_score) -> List[bytes]:
        z = self._zsets.get(key, {})
        lo = float(min_score)
        hi = float(max_score)
        return [m.encode() for m, s in z.items() if lo <= s <= hi]

    async def zscore(self, key: str, member: str) -> Optional[float]:
        m = member.decode() if isinstance(member, bytes) else member
        z = self._zsets.get(key, {})
        return z.get(m)

    async def zrange(self, key: str, start: int, stop: int, withscores: bool = False):
        z = self._zsets.get(key, {})
        sorted_items = sorted(z.items(), key=lambda x: x[1])
        if stop < 0:
            stop = len(sorted_items) + stop + 1
        else:
            stop = stop + 1
        sliced = sorted_items[start:stop]
        if withscores:
            return [(m.encode(), s) for m, s in sliced]
        return [m.encode() for m, _ in sliced]

    # ------------------------------------------------------------------ #
    # Key scan
    # ------------------------------------------------------------------ #

    async def keys(self, pattern: str) -> List[bytes]:
        import fnmatch

        all_keys = (
            list(self._strings.keys())
            + list(self._hashes.keys())
            + list(self._sets.keys())
            + list(self._zsets.keys())
        )
        return [k.encode() for k in set(all_keys) if fnmatch.fnmatch(k, pattern)]

    async def scan(self, cursor: int, match: str = "*", count: int = 100):
        import fnmatch

        all_keys = (
            list(self._strings.keys())
            + list(self._hashes.keys())
            + list(self._sets.keys())
            + list(self._zsets.keys())
        )
        matched = [k.encode() for k in set(all_keys) if fnmatch.fnmatch(k, match)]
        return 0, matched

    # ------------------------------------------------------------------ #
    # Stream commands (minimal)
    # ------------------------------------------------------------------ #

    def _next_stream_id(self, stream: str) -> str:
        ms = int(time.time() * 1000)
        seq = self._stream_seq.get(stream, 0) + 1
        self._stream_seq[stream] = seq
        return f"{ms}-{seq}"

    async def xadd(self, stream: str, fields: Dict, maxlen=None, approximate=False) -> bytes:
        if stream not in self._streams:
            self._streams[stream] = []
        msg_id = self._next_stream_id(stream)
        entry = {
            k.encode() if isinstance(k, str) else k: v.encode() if isinstance(v, str) else v
            for k, v in fields.items()
        }
        self._streams[stream].append((msg_id, entry))
        return msg_id.encode()

    async def xrange(self, stream: str, min="-", max="+", count=None) -> List[Tuple]:
        entries = self._streams.get(stream, [])
        result = []
        for msg_id, data in entries:
            result.append((msg_id.encode(), data))
        if count:
            result = result[:count]
        return result

    async def xlen(self, stream: str) -> int:
        return len(self._streams.get(stream, []))

    async def xgroup_create(
        self, name: str, groupname: str, id: str = "0", mkstream: bool = False
    ) -> bool:
        if mkstream and name not in self._streams:
            self._streams[name] = []
        key = f"{name}:{groupname}"
        if key in self._groups:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        self._groups[key] = {"last_id": id, "pending": []}
        return True

    async def xreadgroup(
        self, groupname: str, consumername: str, streams: Dict, count: int = 10, block: int = None
    ) -> List:
        result = []
        for stream, start in streams.items():
            s = stream.decode() if isinstance(stream, bytes) else stream
            entries = self._streams.get(s, [])
            if not entries:
                continue
            msgs = [
                (msg_id.encode() if isinstance(msg_id, str) else msg_id, data)
                for msg_id, data in entries[-count:]
            ]
            if msgs:
                result.append((s.encode(), msgs))
        return result

    async def xack(self, stream: str, group: str, *msg_ids) -> int:
        return len(msg_ids)

    async def xpending_range(
        self, stream: str, group: str, min="-", max="+", count: int = 100
    ) -> List:
        return []

    async def xclaim(
        self, stream: str, group: str, consumer: str, min_idle_time: int, ids: List
    ) -> List:
        return []

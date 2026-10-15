"""Explicit local test-only Redis lifecycle; never adopts an endpoint.

Linux host principal and Redis executable are trusted. Unrelated principals are
excluded from writing by private generated ACL secrets and filesystem modes.
The only listener is an owned loopback TCP socket. Host compromise / malicious
same-principal code is outside this boundary. No CLI, environment-based ownership grant or startup
hook is provided. Receipts are observations, never importable capabilities.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import stat
import subprocess
import tempfile
import time
from enum import Enum
from typing import Any

import redis.asyncio as redis

PREFIX = "hfa:authority:v1"
HEADER = PREFIX + ":task-owner-header"
OWNERS = PREFIX + ":task-owners"


class Hold(RuntimeError):
    """NOT_PROVEN / HOLD: no activation, reset, adoption or repair is granted."""


class Phase(str, Enum):
    CREATED = "CREATED"
    OWNERSHIP_VERIFIED = "OWNERSHIP_VERIFIED"
    WRITERS_ISOLATED = "WRITERS_ISOLATED"
    FULL_DB_INVENTORY_COMPLETE = "FULL_DB_INVENTORY_COMPLETE"
    EXTERNALLY_ATTESTED = "EXTERNALLY_ATTESTED"
    ACTIVATION_CALLED_ONCE = "ACTIVATION_CALLED_ONCE"
    HEADER_VERIFIED = "HEADER_VERIFIED"
    DOMAIN_RELEASED = "DOMAIN_RELEASED"
    QUIESCENT = "QUIESCENT"
    VERIFIED_RETAINED = "VERIFIED_RETAINED"
    DISPOSED = "DISPOSED"
    HOLD = "HOLD"


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _identity(path: Path) -> tuple[int, int]:
    s = path.stat()
    return s.st_dev, s.st_ino


def _private(path: Path, mode: int) -> None:
    s = path.lstat()
    if stat.S_ISLNK(s.st_mode) or s.st_uid != os.geteuid() or stat.S_IMODE(s.st_mode) != mode:
        raise Hold("private path ownership/mode mismatch")


def process_identity(pid: int) -> dict:
    # /proc can expose an outer PID namespace. Resolve only a child of THIS
    # creator in THIS namespace, never acquire an unrelated process by PID.
    parent = int(next(l.split()[1] for l in Path('/proc/self/status').read_text().splitlines()
                      if l.startswith('Pid:')))
    namespace = os.readlink('/proc/self/ns/pid')
    matches = []
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():
            continue
        try:
            status = dict(l.split(':', 1) for l in (p / 'status').read_text().splitlines())
            if (int(status['NSpid'].split()[-1]) == pid and int(status['PPid']) == parent
                    and os.readlink(p / 'ns/pid') == namespace):
                matches.append(p)
        except (OSError, KeyError):
            continue
    if len(matches) != 1:
        raise Hold('original child / parent / PID namespace mapping unavailable')
    p = matches[0]
    fields = (p / "stat").read_text().rsplit(")", 1)[1].split()
    return {"pid": pid, "proc_pid": int(p.name), "parent_proc_pid": parent,
            "pid_namespace": namespace, "start_ticks": fields[19],
            "exe": str((p / "exe").resolve(strict=True)),
            "exe_identity": list(_identity(p / "exe")),
            "cwd": str((p / "cwd").resolve(strict=True))}


def listener_evidence(proc_pid: int, port: int) -> dict:
    """Host-side proof before any connection; exact launched-child socket inode."""
    inodes = set()
    for fd in Path(f"/proc/{proc_pid}/fd").iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        match = re.fullmatch(r"socket:\[(\d+)\]", target)
        if match:
            inodes.add(match[1])
    listeners = []
    for family in ("tcp", "tcp6"):
        for row in Path(f"/proc/{proc_pid}/net/{family}").read_text().splitlines()[1:]:
            fields = row.split()
            if fields[3] == "0A" and fields[9] in inodes:
                listeners.append((family, fields[1], fields[9]))
    if len(listeners) != 1 or listeners[0][:2] != ('tcp', f'0100007F:{port:04X}'):
        raise Hold("exact loopback listener is not owned exclusively by launched child")
    return {"socket_inode": listeners[0][2], "tcp_listeners": 1,
            "host": "127.0.0.1", "port": port, "transport": "authenticated-loopback"}


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, dict):
        return sorted([[_json_value(k), _json_value(v)] for k, v in value.items()], key=str)
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    return value


async def semantic_snapshot(client: Any) -> list:
    """Quiescent full DB observation, including PEL and absolute TTL deadlines."""
    cursor, keys = 0, set()
    while True:
        cursor, page = await client.scan(cursor=cursor, count=500)
        keys.update(page)
        if int(cursor) == 0:
            break
    result = []
    for key in sorted(keys):
        kind = await client.type(key)
        kind = kind.decode() if isinstance(kind, bytes) else kind
        if kind == "string":
            value = await client.get(key)
        elif kind == "hash":
            value = await client.hgetall(key)
        elif kind == "set":
            value = sorted(await client.smembers(key))
        elif kind == "zset":
            value = await client.zrange(key, 0, -1, withscores=True)
        elif kind == "list":
            value = await client.lrange(key, 0, -1)
        elif kind == "stream":
            groups = []
            for g in await client.xinfo_groups(key):
                name = g.get("name", g.get(b"name"))
                pending = await client.xpending_range(key, name, "-", "+", 100000)
                if int(g.get("pending", g.get(b"pending", 0))) > len(pending):
                    raise Hold("PEL observation incomplete")
                # Idle duration is time-dependent; identity/consumer/delivery count are not.
                rows = [{k: v for k, v in row.items() if k != "time_since_delivered"}
                        for row in pending]
                groups.append([g, rows])
            value = [await client.xrange(key), groups]
        else:
            raise Hold("unreadable or changed key during inventory")
        result.append([_json_value(key), kind, _json_value(value), await client.pexpiretime(key)])
    if await client.dbsize() != len(keys):
        raise Hold("full DB inventory changed or incomplete")
    return result


class OwnedFreshDomain:
    """One creator-held capability. Constructor never starts or activates Redis.

    Only create() can establish host ownership. Domain identity/DB/prefix are not
    caller parameters. A retained verification uses this same live object and its
    original receipts; JSON import, external adoption and process restart do not exist.
    """

    def __init__(self, binary: Path, evidence_parent: Path):
        self._binary = binary.resolve(strict=True)
        self._binary_hash = file_digest(self._binary)
        evidence_parent.mkdir(parents=True, exist_ok=True)
        self.evidence = Path(tempfile.mkdtemp(prefix="domain-", dir=evidence_parent))
        os.chmod(self.evidence, 0o700)
        self._evidence_identity = _identity(self.evidence)
        self._receipts: list[tuple[Path, str]] = []
        self.phase: Phase | None = None
        self._domain_id = "acceptance-" + secrets.token_hex(24)
        self._process: subprocess.Popen | None = None
        self._host: dict | None = None
        self._root: Path | None = None
        self._client = None
        self._scenario = None
        self._activation_attempts = 0
        self._header: dict | None = None
        self._checkpoint: list | None = None
        self._owner_floor: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._owner_password = secrets.token_hex(32)
        self._scenario_password = secrets.token_hex(32)

    @property
    def domain_id(self) -> str:
        return self._domain_id

    def _require(self, *phases: Phase) -> None:
        if self.phase not in phases:
            raise Hold(f"NOT_PROVEN / HOLD: illegal lifecycle state {self.phase}")

    def _record(self, name: str, value: Any) -> None:
        _private(self.evidence, 0o700)
        if _identity(self.evidence) != self._evidence_identity:
            raise Hold("external receipt directory identity changed")
        path = self.evidence / f"{len(self._receipts):03d}-{name}.json"
        raw = json.dumps({"domain_id": self.domain_id, "kind": name, "value": value,
                          "previous_sha256": self._receipts[-1][1] if self._receipts else None},
                         sort_keys=True, indent=2).encode()
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        directory_fd = os.open(self.evidence, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        self._receipts.append((path, hashlib.sha256(raw).hexdigest()))

    def _verify_receipts(self) -> None:
        _private(self.evidence, 0o700)
        if _identity(self.evidence) != self._evidence_identity or not self._receipts:
            raise Hold("original external receipts missing")
        for path, expected in self._receipts:
            _private(path, 0o400)
            if file_digest(path) != expected:
                raise Hold("original external receipt changed")

    def _transition(self, phase: Phase, evidence: Any) -> None:
        self._record(phase.value, evidence)
        self.phase = phase

    def _verify_process(self) -> None:
        if self._process is None or self._host is None or self._process.poll() is not None:
            raise Hold("original child continuity unavailable")
        if process_identity(self._process.pid) != self._host:
            raise Hold("foreign or restarted process")
        if file_digest(self._binary) != self._binary_hash:
            raise Hold("Redis executable changed")
        _private(self._root, 0o700)
        if _identity(self._root) != self._storage_identity:
            raise Hold("storage identity changed")

    def verify_ownership(self) -> dict:
        self._verify_process()
        _private(self._config, 0o400)
        _private(self._acl, 0o400)
        if file_digest(self._config) != self._config_hash or file_digest(self._acl) != self._acl_hash:
            raise Hold("isolation configuration changed")
        listener = listener_evidence(self._host['proc_pid'], self._port)
        if self._listener is not None and listener != self._listener:
            raise Hold("listener generation changed")
        return listener

    async def create(self, *, port: int = 0) -> None:
        if self.phase is not None or self._process is not None:
            raise Hold("no endpoint adoption or second creation")
        version = subprocess.run([str(self._binary), "--version"], check=True,
                                 capture_output=True, text=True, timeout=5).stdout
        if not re.search(r"\bv=7\.4\.10\b", version):
            raise Hold("requires Redis 7.4.10")
        if type(port) is not int or not 0 <= port <= 65535:
            raise Hold('invalid requested port')
        # Bind only: never probe/connect to an occupant. No SO_REUSEADDR. A race
        # after reservation closes must make child startup/ownership fail, never attach.
        reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            reservation.bind(('127.0.0.1', port))
            self._port = reservation.getsockname()[1]
        except OSError as exc:
            reservation.close()
            raise Hold('occupied/unavailable endpoint; no connection or adoption') from exc
        self._root = Path(tempfile.mkdtemp(prefix="hfa-r2-"))
        os.chmod(self._root, 0o700)
        self._storage_identity = _identity(self._root)
        self._config = self._root / "redis.conf"
        self._acl = self._root / "users.acl"
        self._listener = None
        # Secrets appear only as password hashes in a private file, never in argv/receipts.
        self._acl.write_text("user default off\nuser owner on #" +
                             hashlib.sha256(self._owner_password.encode()).hexdigest() +
                             " ~* &* +@all -flushdb -flushall\n")
        os.chmod(self._acl, 0o400)
        self._config.write_text(
            f'bind 127.0.0.1\nport {self._port}\n'
            f'dir "{self._root}"\naclfile "{self._acl}"\n'
            'databases 1\nsave ""\nappendonly no\ndaemonize no\n'
            'maxmemory 0\nmaxmemory-policy noeviction\nprotected-mode yes\n')
        os.chmod(self._config, 0o400)
        self._config_hash, self._acl_hash = file_digest(self._config), file_digest(self._acl)
        self._log = (self.evidence / "redis.log").open("xb")
        reservation.close()
        self._process = subprocess.Popen([str(self._binary), str(self._config)], cwd=self._root,
                                         env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
                                         stdin=subprocess.DEVNULL, stdout=self._log,
                                         stderr=subprocess.STDOUT, start_new_session=True)
        self._host = process_identity(self._process.pid)
        if (self._host['exe_identity'] != list(_identity(self._binary))
                or self._host['exe'] != str(self._binary)
                or self._host['cwd'] != str(self._root)):
            raise Hold('created child executable/storage does not match launch request')
        self._transition(Phase.CREATED, {"host": self._host, "binary_sha256": self._binary_hash,
                                       "launcher_sha256": file_digest(Path(__file__)),
                                       "argv": [str(self._binary), str(self._config)],
                                       "storage_identity": self._storage_identity,
                                       "endpoint": f'127.0.0.1:{self._port}', "db": 0, "prefix": PREFIX,
                                       "config_sha256": self._config_hash, "acl_sha256": self._acl_hash})
        for _ in range(100):
            if self._process.poll() is not None:
                raise Hold("owned Redis startup failed; no endpoint fallback")
            try:
                self._listener = self.verify_ownership()
                break
            except Hold:
                pass
            await asyncio.sleep(0.02)
        else:
            raise Hold("owned listener unavailable")
        self._transition(Phase.OWNERSHIP_VERIFIED, self._listener)
        # First connection follows host verification. No caller URL/PID is accepted.
        self._client = redis.Redis(host='127.0.0.1', port=self._port, db=0,
                                   username="owner", password=self._owner_password,
                                   single_connection_client=True, decode_responses=True,
                                   socket_connect_timeout=2, socket_timeout=2)
        info = await self._client.info("server")
        if info["redis_version"] != "7.4.10" or info["process_id"] != self._process.pid:
            raise Hold("Redis self-report contradicts external ownership")
        self._run_id = info["run_id"]
        self._owner_client_id = await self._client.client_id()
        await self._verify_exclusion()
        self._transition(Phase.WRITERS_ISOLATED, {
            "host_listener": self._listener, "run_id": self._run_id,
            "db": 0, "prefix": PREFIX, "default_user": "off", "scenario_user": "absent",
            "credential_origin": "creator-generated", "private_storage_mode": "0700",
            "tcp_writers": "denied without creator-only ACL credential", "trusted_principal": os.geteuid(),
            "limitation": "malicious same-principal code / compromised host excluded"})

    async def _verify_exclusion(self) -> None:
        self.verify_ownership()
        info = await self._client.info("server")
        clients = await self._client.client_list()
        users = await self._client.acl_users()
        default = await self._client.acl_getuser("default")
        owner = await self._client.acl_getuser("owner")
        if (info["run_id"] != self._run_id or set(users) != {"default", "owner"}
                or "off" not in default["flags"]
                or "on" not in owner["flags"] or "nopass" in owner["flags"]
                or owner["passwords"] != [hashlib.sha256(self._owner_password.encode()).hexdigest()]
                or {int(c["id"]) for c in clients} != {self._owner_client_id}
                or any(c["user"] != "owner" or c["db"] != "0" for c in clients)):
            raise Hold("external writer exclusion not established")

    async def _inventory(self) -> dict:
        await self._verify_exclusion()
        rows = await semantic_snapshot(self._client)
        await self._verify_exclusion()
        evidence = {"cursor_complete": True, "keys": rows, "key_count": len(rows),
                    "sha256": digest(rows), "scope": "entire DB 0"}
        self._record("inventory", evidence)
        if rows:
            raise Hold("pre-existing canonical, legacy or unrelated key; never erase")
        return evidence

    async def activate_fresh(self) -> None:
        async with self._lock:
            self._require(Phase.WRITERS_ISOLATED)
            if self._activation_attempts != 0:
                raise Hold('activation already attempted; no uncertainty retry')
            try:
                inventory = await self._inventory()
                self._transition(Phase.FULL_DB_INVENTORY_COMPLETE, inventory)
                ref = self._receipts[-1]
                self._attestation = {"fresh_domain_id": self.domain_id, "writers_isolated": True,
                                     "attested_by": f"local-launcher:{os.getpid()}:{self.domain_id}",
                                     "evidence_ref": str(ref[0]) + "#sha256=" + ref[1],
                                     "db": 0, "prefix": PREFIX, "run_id": self._run_id,
                                     "host_identity_sha256": digest(self._host)}
                self._verify_receipts()
                self._transition(Phase.EXTERNALLY_ATTESTED, self._attestation)
                # Recheck immediately before the existing API. Credentials remain unreleased.
                await self._verify_exclusion()
                if await self._client.dbsize() != 0:
                    raise Hold("inventory/activation race")
                self._activated_at = time.time_ns() // 1_000_000
                self._activation_attempts += 1
                self._transition(Phase.ACTIVATION_CALLED_ONCE, {"attempt": self._activation_attempts,
                                                               "activated_at_ms": self._activated_at})
                from hfa.authority import RedisCanonicalAuthorityStore
                result = await RedisCanonicalAuthorityStore(self._client).activate_task_owner_namespace(
                    namespace_id=self.domain_id, activated_at_ms=self._activated_at,
                    isolation_attestation=self._attestation)
                self._record("activation_response", result)
                if result != {"status": "ACTIVATED", "inventory_sha256": digest({"namespace": PREFIX, "keys": 0}),
                              "attestation_sha256": digest(self._attestation)}:
                    raise Hold("unexpected activation result (including ALREADY_ACTIVE)")
                self._header = {"schema": "task-owner-header:v1", "state": "ACTIVE",
                                "namespace_id": self.domain_id, "activated_at_ms": str(self._activated_at),
                                "inventory_sha256": result["inventory_sha256"],
                                "attestation_sha256": result["attestation_sha256"], "owner_count": "0"}
                await self._validate_header(fresh=True)
                if set(await self._client.keys("*")) != {HEADER}:
                    raise Hold("unexpected key after activation")
                await self._verify_exclusion()
                self._transition(Phase.HEADER_VERIFIED, self._header)
                await self._release()
            except BaseException as exc:
                self._transition(Phase.HOLD, {"reason": type(exc).__name__, "detail": str(exc),
                                              "activation_attempts": self._activation_attempts})
                raise

    async def _validate_header(self, *, fresh: bool = False) -> None:
        if self._header is None or await self._client.type(HEADER) != "hash":
            raise Hold("missing/wrong-type owner header")
        observed = await self._client.hgetall(HEADER)
        count = observed.get("owner_count", "")
        if (set(observed) != set(self._header) or await self._client.pttl(HEADER) != -1
                or not re.fullmatch(r"0|[1-9][0-9]*", count) or int(count) > 2**53 - 1
                or any(observed[k] != v for k, v in self._header.items() if k != "owner_count")
                or (fresh and count != "0")):
            raise Hold("owner header corrupt or original provenance changed")
        kind = await self._client.type(OWNERS)
        if kind == "none" and count == "0":
            return
        if (kind != "hash" or await self._client.pttl(OWNERS) != -1
                or await self._client.hlen(OWNERS) != int(count)):
            raise Hold("owner inventory lost or inconsistent")

    async def _release(self) -> None:
        self._scenario_password = secrets.token_hex(32)
        await self._client.execute_command(
            "ACL", "SETUSER", "scenario", "reset", "on", ">" + self._scenario_password,
            "~*", "&*", "+@all", "-@admin", "-flushdb", "-flushall", "-restore", "-migrate")
        self._scenario = redis.Redis(host='127.0.0.1', port=self._port, db=0,
                                     username="scenario", password=self._scenario_password,
                                     decode_responses=True, single_connection_client=True,
                                     socket_connect_timeout=2, socket_timeout=2)
        self._transition(Phase.DOMAIN_RELEASED, {"scenario_credential": "new restricted capability",
                                                "activation_attempts": self._activation_attempts})

    @property
    def client(self):
        self._require(Phase.DOMAIN_RELEASED)
        self.verify_ownership()
        return self._scenario

    async def _bijection(self) -> None:
        """Discover from records as well as owners; validate real persisted histories."""
        from hfa.authority import RedisCanonicalAuthorityStore
        from hfa.authority.redis_persistence import _decode_storage_envelope, _record_from_payload
        store = RedisCanonicalAuthorityStore(self._client)
        admissions, identities, known = {}, {}, {HEADER, OWNERS}
        async for key in self._client.scan_iter(match=PREFIX + ":*"):
            if key in {HEADER, OWNERS}:
                continue
            if key.endswith(":operation-records"):
                if await self._client.type(key) != "hash":
                    raise Hold("canonical record inventory type mismatch")
                for raw in (await self._client.hgetall(key)).values():
                    _, payload = _decode_storage_envelope(raw, field_name="inventory record")
                    record = _record_from_payload(payload)
                    identities[record.aggregate_identity_sha256] = record.aggregate_identity
                    if key != store.keyspace(record.aggregate_identity_sha256).operation_records:
                        raise Hold("foreign canonical record key")
        for identity in identities.values():
            keys = store.keyspace(identity.sha256)
            known.update([keys.aggregate, keys.transition_indexes, keys.receipts, keys.operation_records,
                          keys.transition_log, keys.outbox, keys.conflict_index, keys.conflicts,
                          keys.operator_audits])
            history = await store.load_aggregate_history(identity)
            if history is None:
                raise Hold("orphan canonical history")
            for item in history.operations:
                record = item.record
                if record.operation_type == "TASK_ADMIT":
                    task_id = record.aggregate_identity.task_id
                    if task_id in admissions:
                        raise Hold("duplicate global TASK owner")
                    await store.verify_task_owner(record)
                    admissions[task_id] = record
        owners = await self._client.hgetall(OWNERS)
        if set(owners) != set(admissions):
            raise Hold("canonical TASK_ADMIT / owner bijection failed")
        if any(owners.get(task_id) != raw for task_id, raw in self._owner_floor.items()):
            raise Hold('checkpointed owner provenance/count rolled back')
        # Check the stored RUN_CREATE root/tenant relationship without recapturing
        # a live parent head (terminal/progressed RUNs may legitimately be retained).
        from hfa.authority import AggregateType, CanonicalAggregateIdentity
        for record in admissions.values():
            parent = CanonicalAggregateIdentity(aggregate_type=AggregateType.RUN,
                                                run_id=record.aggregate_identity.run_id)
            history = await store.load_aggregate_history(parent)
            creates = [] if history is None else [h.record for h in history.operations
                                                 if h.record.operation_type == 'RUN_CREATE']
            if (len(creates) != 1 or creates[0].authoritative_metadata_changes.get('root_task_id')
                    != record.aggregate_identity.task_id
                    or creates[0].authoritative_metadata_changes.get('tenant_id')
                    != record.authoritative_metadata_changes.get('tenant_id')):
                raise Hold('retained canonical parent/root/tenant evidence mismatch')
        async for key in self._client.scan_iter(match=PREFIX + ":*"):
            if key not in known:
                raise Hold("unaccounted canonical evidence")

    async def quiesce(self) -> None:
        async with self._lock:
            self._require(Phase.DOMAIN_RELEASED)
            # Revocation disconnects every scenario connection, including leaked references.
            await self._client.acl_deluser("scenario")
            await self._scenario.aclose()
            self._scenario = None
            try:
                await self._verify_exclusion()
                await self._validate_header()
                await self._bijection()
                snapshot = await semantic_snapshot(self._client)
                if snapshot != await semantic_snapshot(self._client):
                    raise Hold("checkpoint not quiescent")
                self._transition(Phase.QUIESCENT, {"snapshot": snapshot, "sha256": digest(snapshot)})
                self._checkpoint = snapshot
                self._owner_floor = await self._client.hgetall(OWNERS)
            except BaseException:
                self.phase = Phase.HOLD
                raise

    async def verify_retained(self) -> None:
        async with self._lock:
            self._require(Phase.QUIESCENT, Phase.VERIFIED_RETAINED)
            if self._checkpoint is None:
                raise Hold("NOT_PROVEN / HOLD: original checkpoint unavailable")
            self._verify_receipts()
            await self._verify_exclusion()
            await self._validate_header()
            await self._bijection()
            if await semantic_snapshot(self._client) != self._checkpoint:
                raise Hold("NOT_PROVEN / HOLD: retained checkpoint differs")
            self._transition(Phase.VERIFIED_RETAINED, {"checkpoint_sha256": digest(self._checkpoint),
                                                       "activation_attempts": self._activation_attempts})

    async def resume_verified(self) -> None:
        await self.verify_retained()
        async with self._lock:
            self._require(Phase.VERIFIED_RETAINED)
            await self._release()

    async def reset(self) -> None:
        raise Hold("reset is never an owned-domain capability")

    async def dispose(self) -> None:
        """No endpoint commands, FLUSHDB or PID acquisition during teardown."""
        if self.phase == Phase.DISPOSED:
            raise Hold("already disposed")
        if self.phase == Phase.DOMAIN_RELEASED:
            await self.quiesce()
        if self._process is not None and self.phase != Phase.QUIESCENT:
            self._record('disposal_policy', {'mode': 'explicit owned-child abort or retained disposal',
                                             'phase': self.phase,
                                             'retention_acceptance': False})
        if self._process is not None:
            if self._process.poll() is None:
                self._verify_process()
                self._process.terminate()
            await asyncio.to_thread(self._process.wait, timeout=5)
        if self._scenario is not None:
            await self._scenario.aclose()
        if self._client is not None:
            await self._client.aclose()
        if self._root is not None:
            _private(self._root, 0o700)
            if _identity(self._root) != self._storage_identity:
                raise Hold("refuse disposal of foreign storage")
            shutil.rmtree(self._root)
        if hasattr(self, "_log"):
            self._log.close()
        self._transition(Phase.DISPOSED, {"pid": self._process.pid if self._process else None,
                                         "returncode": self._process.returncode if self._process else None,
                                         "storage_removed": self._root is None or not self._root.exists(),
                                         "activation_attempts": self._activation_attempts})

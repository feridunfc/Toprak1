"""Test-only prepared capabilities and a private-pipe probe protocol.

BOUNDED_TRUSTED_SCENARIO_CODE: the original launcher, its verified children
and audited scenario code are trusted. Malicious same-principal code, hostile
credential holders and compromised hosts are excluded. Product CLI handoff is explicit; no endpoint adoption, proxy, Lua dispatcher
or new authority writer is provided.
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import inspect
import contextlib
import io
import logging
import traceback
import json
import math
import os
from pathlib import Path
import secrets
import signal
import stat
import subprocess
import sys
import time
from typing import Callable

import redis.asyncio as redis

from scripts.acceptance_fresh_domain import (
    HEADER, PREFIX, Hold, OwnedFreshDomain, Phase, digest, file_digest,
    process_identity, semantic_snapshot,
)

_SOURCE = Path(__file__).resolve()
_ISSUERS: dict[object, "PreparedDomainIssuer"] = {}


def _host(proc: Path) -> dict:
    status = dict(line.split(':', 1) for line in (proc / 'status').read_text().splitlines())
    fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
    exe = proc / 'exe'
    s = exe.stat()
    return {'proc_pid': int(status['Pid']), 'pid': int(status['NSpid'].split()[-1]),
            'namespace': os.readlink(proc / 'ns/pid'), 'start_ticks': fields[19],
            'exe': str(exe.resolve()), 'exe_identity': [s.st_dev, s.st_ino]}


def _consumer(consumer: Callable | Path) -> dict:
    if isinstance(consumer, Path):
        path = consumer.resolve(strict=True)
        return {'source': str(path), 'sha256': file_digest(path),
                'executable': str(Path(sys.executable).resolve()),
                'executable_sha256': file_digest(Path(sys.executable))}
    if not inspect.isfunction(consumer):
        raise Hold('consumer must be an exact audited function or probe source')
    path = Path(inspect.getsourcefile(consumer)).resolve(strict=True)
    return {'source': str(path), 'sha256': file_digest(path),
            'function': consumer.__qualname__, 'object_id': id(consumer),
            'code_id': id(consumer.__code__)}


class PreparedGrant:
    """Opaque identity. Values, JSON, dataclasses and shallow copies cannot redeem."""
    __slots__ = ()

    def __new__(cls):
        raise Hold('only a live verified issuer can create a grant')

    def __repr__(self):
        return '<PreparedGrant opaque>'


class ScenarioLease:
    """A separately pooled byte client, without creator or lifecycle capabilities."""
    __slots__ = ('__client', '__valid', '__closed', '__client_closed')

    def __init__(self, client, valid):
        self.__client, self.__valid, self.__closed = client, valid, False
        self.__client_closed = False

    @property
    def client(self):
        if self.__closed:
            raise Hold('scenario lease closed')
        self.__valid()
        return self.__client

    async def aclose(self) -> None:
        self.__closed = True
        if not self.__client_closed:
            await self.__client.aclose()
            self.__client_closed = True

    def __repr__(self):
        return '<ScenarioLease byte-mode>'


async def _ready(fd: int, *, write: bool, deadline: float) -> None:
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    def wake():
        if not future.done():
            future.set_result(None)
    add, remove = (loop.add_writer, loop.remove_writer) if write else (loop.add_reader, loop.remove_reader)
    try:
        add(fd, wake)
        async with asyncio.timeout_at(deadline):
            await future
    finally:
        remove(fd)


def _pipe(fd: int, *, write: bool) -> None:
    if type(fd) is not int or fd <= 2:
        raise Hold('invalid private channel')
    try:
        mode = fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE
        if not stat.S_ISFIFO(os.fstat(fd).st_mode) or mode != (os.O_WRONLY if write else os.O_RDONLY):
            raise Hold('private channel is not the expected pipe end')
        os.set_blocking(fd, False)
    except OSError:
        raise Hold('closed private channel') from None


async def _send(fd: int, value: dict, deadline: float) -> None:
    _pipe(fd, write=True)
    raw = json.dumps(value, sort_keys=True).encode() + b'\n'
    if len(raw) > 16384:
        raise Hold('private frame too large')
    while raw:
        await _ready(fd, write=True, deadline=deadline)
        try:
            n = os.write(fd, raw)
        except BlockingIOError:
            continue
        raw = raw[n:]


async def _receive(fd: int, deadline: float) -> dict:
    _pipe(fd, write=False)
    raw = b''
    while b'\n' not in raw:
        await _ready(fd, write=False, deadline=deadline)
        try:
            part = os.read(fd, 16385)
        except BlockingIOError:
            continue
        if not part:
            raise Hold('parent/child channel EOF')
        raw += part
        if len(raw) > 16384:
            raise Hold('private frame too large')
    if not raw.endswith(b'\n') or raw.count(b'\n') != 1:
        raise Hold('replaced or combined private frame')
    try:
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, UnicodeError):
        raise Hold('invalid private frame') from None


class PreparedDomainIssuer:
    """Creator-held registry; the scenario receives only an opaque grant/lease.

    Caller must explicitly create() and activate_prepared() first. This class
    never activates or repairs a domain. A live event loop enforces deadlines;
    parent hard death/hang and pre-identity startup failures are not covered.
    """
    def __init__(self, domain: OwnedFreshDomain):
        if type(domain) is not OwnedFreshDomain:
            raise Hold('original owned lifecycle required')
        domain._require(Phase.HEADER_VERIFIED)
        if not hasattr(domain, '_prepared_snapshot') or domain._prepared_issued:
            raise Hold('successful fresh preparation required')
        domain.verify_ownership()
        self._domain = domain
        self._creator = _host(Path('/proc/self'))
        self._key = object()
        _ISSUERS[self._key] = self
        self._entry = None
        self._lease = None
        self._timer = None
        self._child = None
        self._child_identity = None
        self._probe_ready = False
        self._communication = None
        self._fds = {}
        self._lock = asyncio.Lock()
        self._closed = asyncio.Event()
        self._error = None

    def __repr__(self):
        return '<PreparedDomainIssuer creator-controlled>'

    def _live(self) -> None:
        if (_ISSUERS.get(self._key) is not self or self._closed.is_set()
                or _host(Path('/proc/self')) != self._creator):
            raise Hold('issuer registry is not live in its original creator')
        if self._entry is not None and time.monotonic() >= self._entry['deadline']:
            raise Hold('prepared grant expired')

    def _binding(self) -> dict:
        d = self._domain
        value = {'domain_id': d.domain_id, 'host': d._host, 'listener': d.verify_ownership(),
                'storage_identity': list(d._storage_identity), 'binary_sha256': d._binary_hash,
                'run_id': d._run_id, 'db': 0, 'prefix': PREFIX,
                'creation': d._receipts[0][1],
                'header': {k: v for k, v in d._header.items() if k != 'owner_count'},
                'activation_receipt': next(h for p, h in d._receipts if 'activation_response' in p.name),
                'setup_sha256': digest(d._prepared_snapshot),
                'setup_receipt': next(h for p, h in d._receipts if 'prepared_setup' in p.name)}
        return json.loads(json.dumps(value))

    async def issue(self, *, scenario: str, acceptance_id: str, consumer: Callable | Path,
                    timeout: float, terminal_events: bool = False) -> PreparedGrant:
        release_failure = None
        async with self._lock:
            self._live()
            d = self._domain
            d._require(Phase.HEADER_VERIFIED)
            if self._entry is not None or d._prepared_issued:
                raise Hold('one grant per fresh preparation')
            if (not all(isinstance(s, str) and s and len(s) <= 128 and
                        all(c.isalnum() or c in '-_.' for c in s) for s in (scenario, acceptance_id))
                    or type(timeout) not in (int, float) or not math.isfinite(timeout)
                    or not 0 < timeout <= 60 or type(terminal_events) is not bool):
                raise Hold('invalid prepared scenario or deadline')
            if terminal_events != d._prepared_terminal:
                raise Hold('required explicit setup is missing or mismatched')
            d._verify_receipts()
            await d._verify_exclusion()
            await d._validate_header(fresh=True)
            if await semantic_snapshot(d._client) != d._prepared_snapshot:
                raise Hold('prepared setup inventory changed')
            binding = self._binding()
            expected_consumer = _consumer(consumer)
            grant = object.__new__(PreparedGrant)
            self._entry = {'grant': grant, 'nonce': secrets.token_hex(32),
                'scenario': scenario, 'acceptance_id': acceptance_id,
                'consumer': expected_consumer, 'binding': binding, 'used': False,
                'deadline': time.monotonic() + timeout}
            d._prepared_issued = True
            try:
                await d._release()
                self._entry['acl_sha256'] = digest(await d._client.acl_getuser('scenario'))
                d._record('grant_issued', {k: v for k, v in self._entry.items()
                                         if k not in {'grant', 'nonce'}} | {
                                             'nonce_sha256': digest(self._entry['nonce'])})
                self._timer = asyncio.create_task(self._expire())
            except BaseException as exc:
                # ACL SETUSER may already have succeeded even if _release,
                # ACL readback, receipt persistence or timer creation failed.
                # Keep the issuer registered until a known-owned abort runs.
                # Never enter _close while holding the issuer lock.
                release_failure = exc
        if release_failure is not None:
            try:
                await self._shield_abort('cancelled' if isinstance(
                    release_failure, asyncio.CancelledError) else 'scenario_failure')
            except asyncio.CancelledError:
                # Repeated caller cancellation must not interrupt owned cleanup.
                # If cleanup succeeded, preserve the cancellation instead of
                # misreporting an ownership HOLD.
                if self._closed.is_set() and self._error is None:
                    raise
                raise Hold('grant release failed; known-owned cleanup HOLD') from None
            except BaseException:
                raise Hold('grant release failed; known-owned cleanup HOLD') from None
            if isinstance(release_failure, asyncio.CancelledError):
                raise asyncio.CancelledError from None
            raise Hold('grant release failed; owned child disposed') from None
        return grant

    async def _validate(self, grant, *, scenario, acceptance_id, consumer) -> dict:
        self._live()
        e, d = self._entry, self._domain
        if (e is None or type(grant) is not PreparedGrant or e['grant'] is not grant
                or e['used'] or scenario != e['scenario'] or acceptance_id != e['acceptance_id']
                or _consumer(consumer) != e['consumer']):
            raise Hold('missing, copied, mismatched or replayed prepared grant')
        d._require(Phase.DOMAIN_RELEASED)
        d._verify_receipts()
        if self._binding() != e['binding']:
            raise Hold('original lifecycle binding changed')
        info = await d._client.info('server')
        if info['run_id'] != d._run_id or info['process_id'] != d._process.pid:
            raise Hold('Redis generation changed')
        if digest(await d._client.acl_getuser('scenario')) != e['acl_sha256']:
            raise Hold('scenario credential privilege changed')
        await d._validate_header(fresh=True)
        if await semantic_snapshot(d._client) != d._prepared_snapshot:
            raise Hold('prepared checkpoint changed before consumer entry')
        self._live()
        return e

    async def redeem(self, grant, *, scenario: str, acceptance_id: str,
                     consumer: Callable) -> ScenarioLease:
        if not inspect.isfunction(consumer):
            raise Hold('in-process redemption requires its exact function consumer')
        async with self._lock:
            e = await self._validate(grant, scenario=scenario,
                                     acceptance_id=acceptance_id, consumer=consumer)
            e['used'] = True
            self._lease = ScenarioLease(self._domain._scenario_bytes(), self._live)
            self._domain._record('grant_consumed', {'transport': 'in_process',
                                                  'nonce_sha256': digest(e['nonce']),
                                                  'consumer': e['consumer']})
            return self._lease

    async def _expire(self) -> None:
        try:
            await asyncio.sleep(max(0, self._entry['deadline'] - time.monotonic()))
            await self.abort(reason='deadline')
        except asyncio.CancelledError:
            raise
        except Exception:
            # Recorded and surfaced by wait_closed/finish/abort; never success.
            self._error = Hold('deadline cleanup failed: HOLD')
            self._closed.set()

    def _verify_child(self, *, source: bool = True) -> None:
        if self._child is None or self._child.poll() is not None:
            raise Hold('registered probe child is no longer live')
        if process_identity(self._child.pid) != self._child_identity:
            raise Hold('registered probe process generation changed')
        expected = self._entry['consumer']
        consumer_source = Path(expected['source'])
        if source and (file_digest(consumer_source) != expected['sha256']
                or self._child_identity['exe'] != expected['executable']
                or file_digest(Path(expected['executable'])) != expected['executable_sha256']):
            raise Hold('probe source or executable identity changed')

    async def run_probe(self, grant, *, scenario: str, acceptance_id: str,
                        behavior: str = 'normal') -> dict:
        """The accepted protocol probe; never claims product CLI acceptance."""
        if behavior not in {'normal', 'exception', 'timeout', 'eof', 'replay'}:
            raise Hold('unknown audited probe behavior')
        return await self._run_child(grant, scenario=scenario, acceptance_id=acceptance_id,
                                     consumer=_SOURCE, behavior=behavior)

    async def run_cli(self, grant, *, scenario: str, acceptance_id: str,
                      consumer: Path, output: Path, json_output: bool = True):
        """Execute the exact grant-bound trusted CLI source using private pipes.

        The caller retains lifecycle ownership. This method never activates or
        adopts a domain. Nonzero scenario exit is returned after verified abort.
        """
        return await self._run_child(grant, scenario=scenario, acceptance_id=acceptance_id,
            consumer=consumer, behavior='product', output=output, json_output=json_output)

    async def _run_child(self, grant, *, scenario, acceptance_id, consumer,
                         behavior, output=None, json_output=True):
        try:
            async with self._lock:
                e = await self._validate(grant, scenario=scenario,
                                         acceptance_id=acceptance_id, consumer=consumer)
                if self._child is not None:
                    raise Hold('probe already registered')
                cr, pw = os.pipe2(os.O_CLOEXEC)
                pr, cw = os.pipe2(os.O_CLOEXEC)
                self._fds.update({fd: (os.fstat(fd).st_dev, os.fstat(fd).st_ino)
                                  for fd in (cr, pw, pr, cw)})
                env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C', 'PYTHONDONTWRITEBYTECODE': '1',
                       'PYTHONPATH': str(_SOURCE.parent.parent)}
                child_args = ([sys.executable, str(consumer), '--prepared-read-fd', str(cr),
                    '--prepared-write-fd', str(cw), '--acceptance-id', acceptance_id,
                    '--out', str(Path(output).resolve())] + (['--json'] if json_output else [])) if behavior == 'product' else [
                    sys.executable, str(_SOURCE), '--probe-child', '--read-fd', str(cr),
                    '--write-fd', str(cw), '--scenario', scenario, '--acceptance-id', acceptance_id]
                self._child = subprocess.Popen(child_args, cwd=_SOURCE.parent.parent, env=env,
                    pass_fds=(cr, cw), stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                self._child_identity = process_identity(self._child.pid)
                for fd in (cr, cw):
                    os.close(fd)
                    self._fds.pop(fd)
                self._verify_child()
                # Drain both pipes concurrently: real JSON reports can exceed pipe capacity.
                self._communication = asyncio.create_task(asyncio.to_thread(self._child.communicate))
                d = self._domain
                d._record('probe_created', {'host': self._child_identity,
                                            'consumer': e['consumer'], 'credential_delivered': False})
                challenge = secrets.token_hex(32)
                channels = {'read_inode': os.fstat(pw).st_ino, 'write_inode': os.fstat(pr).st_ino}
                await _send(pw, {'kind': 'challenge', 'challenge': challenge, 'nonce': e['nonce'],
                    'issuer': self._creator, 'child': _host(Path(f"/proc/{self._child_identity['proc_pid']}")),
                    'consumer': e['consumer'], 'scenario': scenario, 'acceptance_id': acceptance_id,
                    'deadline': e['deadline'], 'binding': e['binding'], 'behavior': behavior,
                    'channels': channels}, e['deadline'])
            response = await _receive(pr, e['deadline'])
            async with self._lock:
                self._verify_child()
                await self._validate(grant, scenario=scenario, acceptance_id=acceptance_id, consumer=consumer)
                if (response.get('kind') != 'response' or response.get('challenge') != challenge
                        or response.get('nonce') != e['nonce'] or response.get('consumer') != e['consumer']
                        or response.get('child') != _host(Path(f"/proc/{self._child_identity['proc_pid']}"))
                        or not isinstance(response.get('child_challenge'), str)):
                    raise Hold('replaced or replayed child challenge')
                e['used'] = True
                await _send(pw, {'kind': 'credential', 'nonce': e['nonce'],
                    'child_challenge': response['child_challenge'], 'host': '127.0.0.1',
                    'port': d._port, 'db': 0, 'username': 'scenario',
                    'password': d._scenario_password}, e['deadline'])
                d._record('grant_consumed', {'transport': 'private_pipes',
                                            'nonce_sha256': digest(e['nonce']),
                                            'consumer': e['consumer'], 'credential_delivered': True})
            ready = await _receive(pr, e['deadline'])
            if ready != {'kind': 'ready', 'corroborated': True}:
                raise Hold('probe boundary corroboration failed')
            self._probe_ready = True
            d._record('probe_ready', {'corroborated': True, 'scenario': scenario,
                                     'acceptance_id': acceptance_id})
            result = await _receive(pr, e['deadline'])
            if behavior == 'product':
                if (set(result) != {'kind', 'returncode', 'exception_type'}
                        or result['kind'] != 'exit' or type(result['returncode']) is not int
                        or not isinstance(result['exception_type'], str)):
                    raise Hold('invalid product CLI exit outcome')
            elif result != {'kind': 'result', 'status': 'PASS', 'dump_bytes': True}:
                raise Hold('probe did not validate byte-mode domain handoff')
            async with asyncio.timeout_at(e['deadline']):
                stdout, stderr = await asyncio.shield(self._communication)
            if behavior == 'product':
                if self._child.returncode != result['returncode']:
                    raise Hold('product CLI exit identity mismatch')
                for raw in (stdout, stderr):
                    if any(secret.encode() in raw for secret in
                           (d._owner_password, d._scenario_password)):
                        raise Hold('credential output refused')
                d._record('product_cli_outcome', result)
                if result['returncode'] == 0:
                    await self.finish()
                else:
                    await self.abort(reason='scenario_failure')
                return subprocess.CompletedProcess(self._child.args, result['returncode'],
                                                   stdout.decode(), stderr.decode())
            if self._child.returncode != 0 or stdout or stderr:
                raise Hold('probe output/exit contract failed')
            await self.finish()
            return {'status': 'PASS', 'probe_only': True, 'dump_bytes': True}
        except asyncio.CancelledError:
            await self._shield_abort('cancelled')
            raise
        except Exception as exc:
            self._domain._record('probe_failure', {'error_type': type(exc).__name__})
            reason = ('deadline' if self._entry is not None and
                      time.monotonic() >= self._entry['deadline'] else 'probe_failure')
            await self._shield_abort(reason)
            raise Hold('probe handoff failed; inspect creator receipts') from None

    async def _reap_child(self) -> None:
        if self._child is None:
            return
        pidfd = None
        try:
            if self._child.poll() is None:
                self._verify_child(source=False)
                pidfd = os.pidfd_open(self._child.pid)
                self._verify_child(source=False)
                signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                try:
                    await asyncio.to_thread(self._child.wait, timeout=2)
                except subprocess.TimeoutExpired:
                    signal.pidfd_send_signal(pidfd, signal.SIGKILL)
                    await asyncio.to_thread(self._child.wait, timeout=2)
            self._domain._record('probe_reaped', {'pid': self._child.pid,
                                                 'returncode': self._child.returncode})
        finally:
            if pidfd is not None:
                os.close(pidfd)
        if self._communication is not None:
            await asyncio.shield(self._communication)
        for stream in (self._child.stdout, self._child.stderr):
            stream.close()

    @staticmethod
    async def _await_owned_cleanup(task: asyncio.Task) -> None:
        """Keep caller cancellation outside the owned cleanup task.

        Every wait, including waits after a first cancel(), must be shielded.
        Retrieve the completed task's exception BEFORE propagating caller
        cancellation: cleanup failure/HOLD must never become a false success.
        This is cooperative, live-event-loop protection, not crash supervision.
        """
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
                # No direct `await task`: another caller cancel() would reach
                # the cleanup coroutine through that unshielded await.
        task.result()  # Propagate real cleanup failure (including cleanup cancellation).
        if cancelled:
            raise asyncio.CancelledError

    async def _shield_abort(self, reason: str) -> None:
        task = asyncio.create_task(self.abort(reason=reason))
        await self._await_owned_cleanup(task)

    async def _shield_close(self, *, reason: str, retain: bool, validate: bool) -> None:
        # _close holds the issuer lock while revoking and disposing resources.
        task = asyncio.create_task(self._close(reason=reason, retain=retain, validate=validate))
        await self._await_owned_cleanup(task)

    async def finish(self, *, retain: bool = False) -> None:
        if self._entry is not None and time.monotonic() >= self._entry['deadline']:
            await self._shield_close(reason='deadline', retain=False, validate=False)
            raise Hold('expired scenario cannot claim normal or retained completion')
        await self._shield_close(reason='normal', retain=retain, validate=True)

    async def abort(self, *, reason: str = 'scenario_failure') -> None:
        # Do not retain caller exception messages or credentials in receipts.
        await self._shield_close(reason=reason if reason in {'deadline', 'cancelled', 'probe_failure'}
                                 else 'scenario_failure', retain=False, validate=False)

    async def wait_closed(self) -> None:
        await self._closed.wait()
        if self._error is not None:
            raise self._error

    async def _close(self, *, reason: str, retain: bool, validate: bool) -> None:
        async with self._lock:
            if self._closed.is_set():
                if self._error:
                    raise self._error
                return
            _ISSUERS.pop(self._key, None)
            if self._timer is not None and self._timer is not asyncio.current_task():
                self._timer.cancel()
            errors = []
            try:
                await self._reap_child()
            except Exception as exc:
                errors.append(type(exc).__name__)
            for fd, expected in self._fds.items():
                try:
                    observed = os.fstat(fd)
                    if (observed.st_dev, observed.st_ino) != expected:
                        raise Hold('private descriptor identity changed; do not close foreign object')
                    os.close(fd)
                except Exception as exc:
                    errors.append(type(exc).__name__)
            self._fds.clear()
            if self._lease is not None:
                try:
                    await self._lease.aclose()
                except Exception as exc:
                    errors.append(type(exc).__name__)
            d = self._domain
            try:
                if validate and not errors:
                    await d.quiesce()
                    if retain:
                        await d.verify_retained()
                if not retain or errors:
                    await d.abort_owned(reason=reason)
                d._record('issuer_closed', {'reason': reason, 'retained': retain and not errors,
                                            'errors': errors})
            except Exception as exc:
                errors.append(type(exc).__name__)
                if d.phase != Phase.DISPOSED:
                    try:
                        await d.abort_owned(reason='validation_failure')
                    except Exception as cleanup:
                        errors.append(type(cleanup).__name__)
            if errors:
                self._error = Hold('issuer cleanup/validation failed: HOLD')
                if d.phase != Phase.DISPOSED:
                    d.phase = Phase.HOLD
                d._record('issuer_hold', {'errors': errors, 'retention_acceptance': False})
            self._closed.set()
            if self._error:
                raise self._error


async def _receive_child_lease(args, *, consumer: Path):
    """Accepted private-channel challenge and read-only Redis corroboration."""
    # A bounded bootstrap wait even if a live parent sends nothing.
    hello = await _receive(args.read_fd, time.monotonic() + 10)
    parent_proc = int(next(l.split()[1] for l in Path('/proc/self/status').read_text().splitlines()
                           if l.startswith('PPid:')))
    if (hello.get('kind') != 'challenge' or hello.get('issuer') != _host(Path(f'/proc/{parent_proc}'))
            or hello.get('child') != _host(Path('/proc/self'))
            or hello.get('consumer') != _consumer(consumer)
            or hello.get('channels') != {'read_inode': os.fstat(args.read_fd).st_ino,
                                         'write_inode': os.fstat(args.write_fd).st_ino}
            or hello.get('scenario') != args.scenario or hello.get('acceptance_id') != args.acceptance_id
            or not isinstance(hello.get('deadline'), (int, float))
            or not 0 < hello['deadline'] - time.monotonic() <= 60):
        raise Hold('invalid issuer, child or source binding')
    deadline, nonce = hello['deadline'], hello['nonce']
    if hello['behavior'] == 'eof':
        return None
    child_challenge = secrets.token_hex(32)
    await _send(args.write_fd, {'kind': 'response', 'challenge': hello['challenge'],
        'nonce': 'replayed' if hello['behavior'] == 'replay' else nonce,
        'consumer': _consumer(consumer), 'child': _host(Path('/proc/self')),
        'child_challenge': child_challenge}, deadline)
    credential = await _receive(args.read_fd, deadline)
    if (credential.get('kind') != 'credential' or credential.get('nonce') != nonce
            or credential.get('child_challenge') != child_challenge
            or credential.get('db') != 0 or credential.get('host') != '127.0.0.1'
            or credential.get('port') != hello['binding']['listener']['port']):
        raise Hold('replaced credential handoff')
    client = redis.Redis(host=credential['host'], port=credential['port'], db=0,
        username=credential['username'], password=credential['password'], decode_responses=False,
        socket_connect_timeout=2, socket_timeout=2)
    try:
        info = await client.info('server')
        binding = hello['binding']
        header = {k.decode(): v.decode() for k, v in (await client.hgetall(HEADER)).items()}
        corroboration = redis.Redis(host=credential['host'], port=credential['port'], db=0,
            username=credential['username'], password=credential['password'], decode_responses=True,
            socket_connect_timeout=2, socket_timeout=2)
        try:
            setup_sha256 = digest(await semantic_snapshot(corroboration))
        finally:
            await corroboration.aclose()
        if (info['run_id'] != binding['run_id'] or info['process_id'] != binding['host']['pid']
                or header != {**binding['header'], 'owner_count': '0'}
                or await client.pttl(HEADER) != -1
                or binding['db'] != 0 or binding['prefix'] != PREFIX
                or setup_sha256 != binding['setup_sha256']):
            raise Hold('read-only CLI boundary corroboration failed')
        await _send(args.write_fd, {'kind': 'ready', 'corroborated': True}, deadline)
        def valid():
            if time.monotonic() >= deadline or _host(Path(f'/proc/{parent_proc}')) != hello['issuer']:
                raise Hold('CLI grant expired or original issuer lost')
        return ScenarioLease(client, valid), hello, credential
    except BaseException:
        await client.aclose()
        raise


async def _probe_child(args) -> None:
    received = await _receive_child_lease(args, consumer=_SOURCE)
    if received is None:
        return
    lease, hello, _credential = received
    try:
        client = lease.client
        deadline = hello['deadline']
        if hello['behavior'] == 'exception':
            raise Hold('audited scenario exception')
        if hello['behavior'] == 'timeout':
            await asyncio.sleep(60)
        if not isinstance(await client.dump(HEADER), bytes):
            raise Hold('DUMP byte contract failed')
        await _send(args.write_fd, {'kind': 'result', 'status': 'PASS', 'dump_bytes': True}, deadline)
    finally:
        await lease.aclose()


_CHILD_GRANTS: dict[PreparedGrant, dict] = {}


async def borrow_builder_lease(*, grant, issuer, consumer, scenario, acceptance_id,
                               redis_url=None, reset_test_db=False) -> ScenarioLease:
    """No URL, reset flag, forged lease or copied receipt can authorize entry."""
    if redis_url is not None or reset_test_db is not False:
        raise Hold('URL-only/reset execution is forbidden; explicit prepared grant required')
    if type(grant) is not PreparedGrant:
        raise Hold('explicit live prepared grant required')
    if type(issuer) is PreparedDomainIssuer:
        if _ISSUERS.get(getattr(issuer, '_key', None)) is not issuer:
            raise Hold('original live issuer required')
        return await issuer.redeem(grant, scenario=scenario, acceptance_id=acceptance_id,
                                   consumer=consumer)
    e = _CHILD_GRANTS.get(grant)
    if (issuer is not None or e is None or e['used'] or e['consumer'] is not consumer
            or e['scenario'] != scenario or e['acceptance_id'] != acceptance_id):
        raise Hold('missing, foreign or reused child prepared grant')
    e['lease'].client  # Revalidate live original parent and deadline before product writes.
    e['used'] = True
    return e['lease']


async def prepare_fresh_product_domain(*, grant, issuer, consumer, scenario, acceptance_id) -> ScenarioLease:
    """One explicit local creator action before starting normal product services.

    Only the original live issuer and its unused fresh grant authorize this
    step. Retained endpoints, URLs and copied receipts cannot enter. Original
    activation/setup receipts stay intact; subsequent setup gets its own receipt.
    """
    if (type(issuer) is not PreparedDomainIssuer
            or _ISSUERS.get(getattr(issuer, '_key', None)) is not issuer
            or not issuer._domain._prepared_terminal):
        raise Hold('original fresh issuer with terminal preparation required')
    lease = await borrow_builder_lease(grant=grant, issuer=issuer, consumer=consumer,
        scenario=scenario, acceptance_id=acceptance_id)
    try:
        from hfa.config.keys import RedisKey
        from hfa_control.product_startup import verify_terminal_event_readiness
        from hfa_control.run_terminal_event_evidence import ensure_terminal_event_index, backfill_terminal_event_evidence
        client = lease.client
        if await client.exists(RedisKey.stream_results()):
            raise Hold('fresh product preparation refuses an existing results stream')
        if await ensure_terminal_event_index(client) != 'already_prepared':
            raise Hold('original prepared terminal contract required')
        await client.xgroup_create(RedisKey.stream_results(), 'single-task-product-bootstrap', id='0', mkstream=True)
        result = await backfill_terminal_event_evidence(client, completed_at_ms=time.time_ns() // 1_000_000)
        if (result.status != 'ready' or result.scanned_entries or result.terminal_events
                or result.evidence_created or result.snapshot_last_entry_id != '0-0'):
            raise Hold('fresh product preparation did not validate empty source history')
        ready = await verify_terminal_event_readiness(client)
        await issuer._domain._validate_header(fresh=True)
        issuer._domain._record('fresh_product_prepared', {
            'readiness': ready, 'backfill': vars(result), 'work_admitted': False})
        return lease
    except BaseException as exc:
        # Reuse the accepted repeated-cancellation-safe owner cleanup. It closes
        # this lease too; an unshielded preliminary close must not strand Redis.
        await issuer._shield_abort('cancelled' if isinstance(exc, asyncio.CancelledError)
                                   else 'scenario_failure')
        raise


async def product_cli_child(args, *, consumer, scenario, execute) -> int:
    """Private entry only. Captured output is redacted before reaching OS pipes."""
    channel_args = argparse.Namespace(read_fd=args.prepared_read_fd,
        write_fd=args.prepared_write_fd, scenario=scenario, acceptance_id=args.acceptance_id)
    received = await _receive_child_lease(channel_args, consumer=Path(inspect.getsourcefile(consumer)))
    if received is None:
        raise Hold('missing product child handshake')
    lease, hello, credential = received
    grant = object.__new__(PreparedGrant)
    _CHILD_GRANTS[grant] = {'lease': lease, 'consumer': consumer, 'scenario': scenario,
                          'acceptance_id': args.acceptance_id, 'used': False}
    out, err = io.StringIO(), io.StringIO()
    handlers = {h for logger in [logging.getLogger(), *[
        x for x in logging.Logger.manager.loggerDict.values() if isinstance(x, logging.Logger)]]
        for h in logger.handlers if isinstance(h, logging.StreamHandler) and h.stream in (sys.stdout, sys.stderr)}
    streams = {h: h.stream for h in handlers}
    code, error_type = 1, ''
    try:
        for h in handlers:
            h.setStream(err if streams[h] is sys.stderr else out)
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = await execute(args, grant, None)
                if type(code) is not int:
                    raise Hold('invalid original CLI status')
            except Exception as exc:
                error_type = type(exc).__name__
                traceback.print_exc()
                code = 1
    finally:
        _CHILD_GRANTS.pop(grant, None)
        await lease.aclose()
        for h, stream in streams.items():
            h.setStream(stream)
    def redacted(value):
        return value.replace(credential['password'], '<redacted>')
    sys.stdout.write(redacted(out.getvalue()))
    sys.stderr.write(redacted(err.getvalue()))
    await _send(channel_args.write_fd, {'kind': 'exit', 'returncode': code,
                                      'exception_type': error_type}, hello['deadline'])
    return code


async def launch_product_cli(args, *, source: Path, scenario: str) -> int:
    """Explicit creator action; normal builders cannot call or infer this grant."""
    if args.redis_url is not None or args.reset_test_db:
        raise Hold('explicit owned launcher rejects URL/reset flags')
    binary = Path(os.environ['HFA_ACCEPTANCE_REDIS_BINARY'])
    evidence = Path(os.environ['HFA_ACCEPTANCE_EVIDENCE_DIR'])
    domain = OwnedFreshDomain(binary, evidence)
    issuer = None
    try:
        await domain.create()
        await domain.activate_prepared(terminal_events=True)
        issuer = PreparedDomainIssuer(domain)
        grant = await issuer.issue(scenario=scenario, acceptance_id=args.acceptance_id,
                                   consumer=source, timeout=60, terminal_events=True)
        completed = await issuer.run_cli(grant, scenario=scenario, acceptance_id=args.acceptance_id,
                                         consumer=source, output=Path(args.out), json_output=args.json)
        sys.stdout.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        return completed.returncode
    finally:
        if issuer is not None and not issuer._closed.is_set():
            await issuer.abort()
        elif issuer is None and domain._host is not None and domain.phase != Phase.DISPOSED:
            await PreparedDomainIssuer._await_owned_cleanup(
                asyncio.create_task(domain.abort_owned(reason='scenario_failure')))


def add_product_cli_arguments(parser) -> None:
    parser.add_argument('--owned-fresh-domain', action='store_true')
    parser.add_argument('--prepared-read-fd', type=int)
    parser.add_argument('--prepared-write-fd', type=int)


async def dispatch_product_cli(args, *, source, scenario, consumer, execute) -> int:
    if args.owned_fresh_domain:
        if args.prepared_read_fd is not None or args.prepared_write_fd is not None:
            raise Hold('launcher/child modes cannot be combined')
        return await launch_product_cli(args, source=source, scenario=scenario)
    if (args.prepared_read_fd is not None and args.prepared_write_fd is not None
            and args.redis_url is None and not args.reset_test_db):
        return await product_cli_child(args, consumer=consumer, scenario=scenario, execute=execute)
    raise Hold('explicit owned launcher or live private child handoff required')


def main() -> int:
    parser = argparse.ArgumentParser(description='Test-only private-channel probe; no product CLI')
    parser.add_argument('--probe-child', action='store_true', required=True)
    parser.add_argument('--read-fd', type=int, required=True)
    parser.add_argument('--write-fd', type=int, required=True)
    parser.add_argument('--scenario', required=True)
    parser.add_argument('--acceptance-id', required=True)
    args = parser.parse_args()
    try:
        asyncio.run(_probe_child(args))
        return 0
    except BaseException:
        # No traceback/connection repr/credential may escape the private channel.
        return 2


if __name__ == '__main__':
    raise SystemExit(main())

"""Test-only, creator-owned legacy Redis and authenticated pytest transport.

BOUNDED_TRUSTED_SCENARIO_CODE: audited launcher/tests and host are trusted;
malicious same-principal code and parent hard death are outside the guarantee.
No prepared-domain activation, endpoint adoption or fixture replacement occurs.
Expected executable SHA256 is a required reviewed input, never inferred as trust.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import weakref

import pytest
import redis.asyncio as redis

# Direct CLI and ``python -m`` have the same repository-relative imports.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.acceptance_fresh_domain import (
    HEADER, OWNERS, Hold, _identity, _private, file_digest,
    listener_evidence, process_identity,
)
from scripts.acceptance_prepared_domain import (
    PreparedDomainIssuer, _host, _pipe, _receive, _send,
)

SOURCE = Path(__file__).resolve()
PLUGIN = "scripts.acceptance_ci_legacy_environment"
READ_FD = "HFA_CI_LEGACY_READ_FD"
WRITE_FD = "HFA_CI_LEGACY_WRITE_FD"


def reviewed_binary(path: Path, expected_sha256: str) -> dict:
    """Validate identity before executing even --version; never select a binary."""
    if not isinstance(path, Path) or not path.is_absolute():
        raise Hold("absolute reviewed Redis executable required")
    if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise Hold("reviewed expected SHA256 required")
    try:
        if (path != path.resolve(strict=True) or path.is_symlink()
                or not stat.S_ISREG(path.stat().st_mode) or not os.access(path, os.X_OK)):
            raise Hold("reviewed executable must be a regular executable file")
        actual = file_digest(path)
        if actual != expected_sha256:
            raise Hold("reviewed executable digest mismatch")
        if sys.platform != "linux" or not all(hasattr(os, n) for n in ("pidfd_open", "pipe2")):
            raise Hold("Linux proc/pidfd/private pipe compatibility required")
        if not Path('/proc/self/stat').is_file():
            raise Hold("host process evidence unavailable")
        version = subprocess.run([str(path), '--version'], capture_output=True,
                                 text=True, check=True, timeout=5).stdout
        if not re.search(r'\bv=7\.4\.10\b', version):
            raise Hold("requires reviewed Redis 7.4.10")
        if file_digest(path) != expected_sha256:
            raise Hold("executable changed during validation")
        return {'path': str(path), 'sha256': actual, 'reviewed_sha256': expected_sha256,
                'file_identity': list(_identity(path)), 'version': version.strip(),
                'platform': platform.platform(), 'machine': platform.machine()}
    except Hold:
        raise
    except Exception:
        raise Hold("reviewed binary unavailable or incompatible") from None


class OwnedLegacyEnvironment:
    """One newly created Redis, private legacy credentials, registered pytest child.

    ``run_pytest`` forwards the exact argument vector and exit code. It consumes
    no caller ownership manifest. A live two-way inherited-pipe exchange binds
    the original child, parent, code, deadline and original Redis creation proof.
    """

    def __init__(self, binary: Path, *, expected_sha256: str, evidence_parent: Path,
                 port: int = 6389):
        if type(port) is not int or not 0 <= port <= 65535:
            raise Hold('invalid legacy port')
        self.binary, self.expected_sha256, self.port = binary, expected_sha256, port
        parent = Path(evidence_parent)
        parent.mkdir(parents=True, exist_ok=True)
        self.evidence = Path(tempfile.mkdtemp(prefix='legacy-', dir=parent))
        os.chmod(self.evidence, 0o700)
        self._creator_password, self._legacy_password = secrets.token_hex(32), secrets.token_hex(32)
        self.process = self.host = self.root = self.client = self.listener = None
        self.child = self.child_identity = self._communication = None
        self._fds, self._records, self._previous = {}, [], None
        self._source_hash = file_digest(SOURCE)
        self._creator = _host(Path('/proc/self'))
        self._closed = False
        self._cleanup_error = False
        self._cleanup_lock = asyncio.Lock()
        self._active = None
        self._credential_live = False

    def __repr__(self):
        return '<OwnedLegacyEnvironment creator-controlled>'

    @property
    def endpoint(self):
        return f'redis://127.0.0.1:{self.port}/0'

    def _record(self, kind, value):
        row = {'kind': kind, 'value': value, 'previous_sha256': self._previous}
        raw = (json.dumps(row, sort_keys=True, indent=2) + '\n').encode()
        if any(s.encode() in raw for s in (self._creator_password, self._legacy_password)):
            raise Hold('credential forbidden in receipt')
        path = self.evidence / f'{len(self._records):03d}-{kind}.json'
        with path.open('xb') as stream:
            stream.write(raw)
        self._previous = hashlib.sha256(raw).hexdigest()
        self._records.append(path)

    def _storage(self):
        _private(self.root, 0o700)
        if (_identity(self.root) != self.storage_identity
                or file_digest(self.config) != self.config_hash
                or file_digest(self.acl) != self.acl_hash):
            raise Hold('original legacy storage/configuration identity lost')

    def _verify_receipts(self):
        expected = self._previous
        for path in reversed(self._records):
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != expected:
                raise Hold('original immutable legacy receipt chain lost')
            expected = json.loads(raw)['previous_sha256']
        if expected is not None:
            raise Hold('incomplete original legacy receipt chain')

    def verify(self):
        self._verify_receipts()
        if self._closed or self.process is None or self.host is None or self.process.poll() is not None:
            raise Hold('original live legacy child unavailable')
        if process_identity(self.process.pid) != self.host:
            raise Hold('legacy process generation/identity lost')
        self._storage()
        if file_digest(self.binary) != self.expected_sha256:
            raise Hold('reviewed executable identity lost')
        observed = listener_evidence(self.host['proc_pid'], self.port)
        if self.listener is not None and observed != self.listener:
            raise Hold('original legacy listener identity lost')
        return observed

    async def create(self):
        if self.process is not None or self.root is not None or self._closed:
            raise Hold('no reuse, adoption or second creation')
        binary = reviewed_binary(self.binary, self.expected_sha256)
        self._record('BINARY_VERIFIED', binary)
        reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            reservation.bind(('127.0.0.1', self.port))
            self.port = reservation.getsockname()[1]
        except OSError:
            reservation.close()
            self._record('OCCUPIED_ENDPOINT', {'connection_attempted': False})
            raise Hold('occupied legacy endpoint; no connection or adoption') from None
        try:
            self.root = Path(tempfile.mkdtemp(prefix='hfa-ci-legacy-'))
            os.chmod(self.root, 0o700)
            self.storage_identity = _identity(self.root)
            self.config, self.acl = self.root/'redis.conf', self.root/'users.acl'
            self.acl.write_text('user default off\nuser legacy_creator on #' +
                hashlib.sha256(self._creator_password.encode()).hexdigest() + ' ~* &* +@all\n' +
                'user legacy_fixture on #' + hashlib.sha256(self._legacy_password.encode()).hexdigest() +
                ' ~* &* +@all -@admin -flushall -restore -migrate +flushdb\n')
            self.config.write_text(f'bind 127.0.0.1\nport {self.port}\ndir "{self.root}"\n'
                f'aclfile "{self.acl}"\ndatabases 1\nsave ""\nappendonly no\ndaemonize no\n'
                'protected-mode yes\nmaxmemory 0\nmaxmemory-policy noeviction\n')
            os.chmod(self.acl, 0o400)
            os.chmod(self.config, 0o400)
            self.config_hash, self.acl_hash = file_digest(self.config), file_digest(self.acl)
            self.log = (self.evidence/'redis.log').open('xb')
            reservation.close()
            self.process = subprocess.Popen([str(self.binary), str(self.config)], cwd=self.root,
                env={'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'}, stdin=subprocess.DEVNULL,
                stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
            self.host = process_identity(self.process.pid)
            if (self.host['exe'] != str(self.binary) or self.host['cwd'] != str(self.root)
                    or self.host['exe_identity'] != binary['file_identity']):
                raise Hold('created Redis executable/storage mismatch')
            self._record('CREATED', {'host': self.host, 'storage': str(self.root),
                'launcher_sha256': self._source_hash,
                'python': {'path': str(Path(sys.executable).resolve()),
                           'sha256': file_digest(Path(sys.executable)), 'version': sys.version},
                'storage_identity': list(self.storage_identity), 'binary': binary,
                'config_sha256': self.config_hash, 'acl_sha256': self.acl_hash,
                'endpoint': self.endpoint, 'activation_permitted': False})
            for _ in range(150):
                try:
                    self.listener = self.verify()
                    break
                except Hold:
                    if self.process.poll() is not None:
                        raise
                    await asyncio.sleep(.02)
            else:
                raise Hold('owned legacy listener unavailable')
            self._record('OWNERSHIP_VERIFIED', {'host': self.host, 'listener': self.listener})
            # The first connection follows external ownership verification.
            self.client = redis.Redis(host='127.0.0.1', port=self.port, db=0,
                username='legacy_creator', password=self._creator_password,
                decode_responses=True, single_connection_client=True,
                socket_connect_timeout=2, socket_timeout=2)
            info = await self.client.info('server')
            if info['process_id'] != self.process.pid or info['redis_version'] != '7.4.10':
                raise Hold('server identity contradicts host evidence')
            self.run_id = info['run_id']
            if await self.client.dbsize() != 0:
                raise Hold('new legacy child is not empty; no reset permitted')
            users = {u: await self.client.acl_getuser(u) for u in await self.client.acl_users()}
            if (set(users) != {'default', 'legacy_creator', 'legacy_fixture'}
                    or 'off' not in users['default']['flags']
                    or users['legacy_fixture']['passwords'] != [hashlib.sha256(self._legacy_password.encode()).hexdigest()]):
                raise Hold('legacy credential/isolation configuration mismatch')
            checks = {}
            for label, command in [('flushdb', ['FLUSHDB']), ('flushall', ['FLUSHALL']),
                                   ('acl', ['ACL', 'LIST']), ('info', ['INFO', 'server'])]:
                try:
                    response = await self.client.execute_command('ACL', 'DRYRUN', 'legacy_fixture', *command)
                    checks[label] = ('DENIED' if isinstance(response, str) and
                        response.startswith("User legacy_fixture has no permissions to run ") else response)
                except redis.ResponseError:
                    checks[label] = 'DENIED'
            self._record('ACL_PRIVILEGE_PROBE', checks)
            if checks != {'flushdb': 'OK', 'flushall': 'DENIED', 'acl': 'DENIED', 'info': 'OK'}:
                raise Hold('legacy ACL privilege contract mismatch')
            if any(c['user'] != 'legacy_creator' for c in await self.client.client_list()):
                raise Hold('writer exclusion not established')
            self._credential_live = True
            self._record('WRITERS_ISOLATED', {'run_id': self.run_id, 'acl': checks,
                'default_user': 'off', 'credential_origin': 'new legacy-only creator secrets',
                'activation_count': 0, 'trust': 'trusted host/audited code; malicious same-principal excluded'})
        except BaseException:
            if self.process is not None and self.host is not None:
                await self.close()
            elif self.process is None and self.root is not None:
                self._storage()
                shutil.rmtree(self.root)
                self.log.close() if hasattr(self, 'log') else None
            else:
                self._record('STARTUP_HOLD', {'unverified_cleanup_permitted': False})
            raise
        finally:
            reservation.close()

    def _binding(self):
        self.verify()
        return {'kind': 'OWNED_LEGACY', 'host': self.host, 'listener': self.listener,
            'storage': str(self.root), 'storage_identity': list(self.storage_identity),
            'config': str(self.config), 'config_sha256': self.config_hash,
            'acl': str(self.acl), 'acl_sha256': self.acl_hash, 'run_id': self.run_id,
            'endpoint': self.endpoint, 'db': 0, 'binary_sha256': self.expected_sha256,
            'creation_sha256': hashlib.sha256(self._records[1].read_bytes()).hexdigest()}

    def _verify_child(self):
        if self.child is None or self.child_identity is None:
            raise Hold('unregistered pytest child')
        if self.child.poll() is None and process_identity(self.child.pid) != self.child_identity:
            raise Hold('pytest child generation/identity lost')

    async def run_pytest(self, arguments, *, cwd: Path = ROOT, timeout: float = 300,
                         environment: dict | None = None):
        if (not isinstance(arguments, (list, tuple)) or not all(type(a) is str and '\0' not in a for a in arguments)
                or type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 3600):
            raise Hold('invalid original pytest argument vector/deadline')
        if self._active is not None or self.child is not None:
            raise Hold('one original pytest execution per legacy lifecycle')
        self.verify()
        env = dict(os.environ if environment is None else environment)
        if READ_FD in env or WRITE_FD in env:
            raise Hold('caller FD flags are not ownership authority')
        if any(s in repr(env) + repr(arguments) for s in (self._creator_password, self._legacy_password)):
            raise Hold('credential in caller arguments/environment')
        deadline = time.monotonic() + timeout
        cr, pw = os.pipe2(os.O_CLOEXEC)
        pr, cw = os.pipe2(os.O_CLOEXEC)
        self._fds = {fd: (os.fstat(fd).st_dev, os.fstat(fd).st_ino) for fd in (cr, pw, pr, cw)}
        self._active = {'nonce': secrets.token_hex(32), 'used': False, 'deadline': deadline}
        env.update({READ_FD: str(cr), WRITE_FD: str(cw), 'USE_EXISTING_REDIS': '1',
                    'PYTHONDONTWRITEBYTECODE': '1'})
        env['PYTEST_PLUGINS'] = ','.join(filter(None, [env.get('PYTEST_PLUGINS'), PLUGIN]))
        env['PYTHONPATH'] = str(ROOT) + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
        argv = [sys.executable, '-m', 'pytest', *arguments]
        try:
            if file_digest(SOURCE) != self._source_hash:
                raise Hold('audited launcher source changed')
            self.child = subprocess.Popen(argv, cwd=Path(cwd).resolve(strict=True), env=env,
                pass_fds=(cr, cw), stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            self.child_identity = process_identity(self.child.pid)
            self._record('PYTEST_CREATED', {'host': self.child_identity, 'argv': argv,
                'arguments': list(arguments), 'source_sha256': self._source_hash,
                'credentials_in_argv_or_environment': False})
            self._verify_child()
            for fd in (cr, cw):
                os.close(fd)
                self._fds.pop(fd)
            self._communication = asyncio.create_task(asyncio.to_thread(self.child.communicate))
            handshake_deadline = min(deadline, time.monotonic() + 15)
            hello = {'kind': 'LEGACY_CHALLENGE', 'issuer': self._creator,
                'child': _host(Path(f'/proc/{self.child_identity["proc_pid"]}')),
                'source_sha256': self._source_hash, 'nonce': self._active['nonce'],
                'deadline': deadline, 'binding': self._binding(), 'arguments': list(arguments),
                'read_inode': os.fstat(pw).st_ino, 'write_inode': os.fstat(pr).st_ino}
            await _send(pw, hello, handshake_deadline)
            response = await _receive(pr, handshake_deadline)
            self._verify_child()
            if (self._active['used'] or response.get('kind') != 'LEGACY_RESPONSE'
                    or response.get('nonce') != hello['nonce'] or response.get('child') != hello['child']
                    or response.get('source_sha256') != self._source_hash
                    or not re.fullmatch('[0-9a-f]{64}', response.get('challenge', ''))):
                raise Hold('legacy child challenge/source mismatch')
            self.verify()
            self._active['used'] = True
            await _send(pw, {'kind': 'LEGACY_CREDENTIAL', 'nonce': hello['nonce'],
                'challenge': response['challenge'], 'username': 'legacy_fixture',
                'password': self._legacy_password}, handshake_deadline)
            ready = await _receive(pr, handshake_deadline)
            if ready != {'kind': 'LEGACY_READY', 'nonce': hello['nonce']}:
                raise Hold('legacy child did not corroborate original domain')
            self._record('CREDENTIAL_RELEASED', {'username': 'legacy_fixture',
                'child': self.child_identity, 'one_use': True, 'deadline': deadline})
            async with asyncio.timeout_at(deadline):
                stdout, stderr = await asyncio.shield(self._communication)
            self._record('PYTEST_EXIT', {'host': self.child_identity, 'returncode': self.child.returncode})
            result = subprocess.CompletedProcess(argv, self.child.returncode,
                self.redact(stdout.decode(errors='replace')), self.redact(stderr.decode(errors='replace')))
        except Hold:
            # pytest may reject its original arguments before plugin configure.
            # Preserve that real nonzero exit without ever issuing credentials.
            if self._communication is not None and not self._active['used']:
                try:
                    async with asyncio.timeout(.5):
                        stdout, stderr = await asyncio.shield(self._communication)
                except TimeoutError:
                    pass
                else:
                    if self.child.returncode != 0:
                        self._record('PYTEST_STARTUP_EXIT', {'returncode': self.child.returncode,
                            'credential_released': False})
                        result = subprocess.CompletedProcess(argv, self.child.returncode,
                            self.redact(stdout.decode(errors='replace')),
                            self.redact(stderr.decode(errors='replace')))
                        await self.close()
                        return result
            await self.close()
            raise
        except BaseException:
            await self.close()
            raise
        else:
            await self.close()
            return result

    def redact(self, text):
        for secret in (self._creator_password, self._legacy_password):
            text = text.replace(secret, '<redacted>')
        return text

    async def close(self):
        # Reuse the accepted repeated-cancellation/HOLD-precedence implementation.
        await PreparedDomainIssuer._await_owned_cleanup(asyncio.create_task(self._cleanup()))

    async def _cleanup(self):
        async with self._cleanup_lock:
            await self._cleanup_body()

    async def _cleanup_body(self):
        if self._closed:
            if self._cleanup_error:
                raise Hold('previous legacy cleanup failure: HOLD')
            return
        errors = []
        redis_fd = child_fd = None
        try:
            # Verify *all* identities before any signal, ACL write or deletion.
            if self.process is not None:
                self._verify_receipts()
                self._storage()
                if self.process.poll() is None:
                    self.verify()
                    redis_fd = os.pidfd_open(self.process.pid)
                    self.verify()
            if self.child is not None:
                self._verify_child()
                if self.child.poll() is None:
                    child_fd = os.pidfd_open(self.child.pid)
                    self._verify_child()
            if self.client is not None and redis_fd is not None:
                try:
                    await self.client.acl_setuser('legacy_fixture', enabled=False)
                    await self.client.client_kill_filter(user='legacy_fixture')
                    removed = await self.client.acl_deluser('legacy_fixture')
                    self._credential_live = False
                    self._record('CREDENTIAL_REVOKED', {'username': 'legacy_fixture', 'removed': bool(removed)})
                except Exception as exc:
                    errors.append(type(exc).__name__)
            for proc, fd, kind in [(self.child, child_fd, 'PYTEST_REAPED'),
                                   (self.process, redis_fd, 'REDIS_REAPED')]:
                if proc is None:
                    continue
                if fd is not None:
                    signal.pidfd_send_signal(fd, signal.SIGTERM)
                    try:
                        await asyncio.to_thread(proc.wait, timeout=2)
                    except subprocess.TimeoutExpired:
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                        await asyncio.to_thread(proc.wait, timeout=2)
                self._record(kind, {'pid': proc.pid, 'returncode': proc.poll()})
            if self._communication is not None:
                await asyncio.shield(self._communication)
            if self.root is not None and (self.root/'pytest-observation.json').exists():
                observation = (self.root/'pytest-observation.json').read_text()
                if any(s in observation for s in (self._creator_password, self._legacy_password)):
                    raise Hold('credential in child observation: HOLD')
                (self.evidence/'pytest-observation.json').write_text(observation)
            for fd, identity in list(self._fds.items()):
                s = os.fstat(fd)
                if (s.st_dev, s.st_ino) != identity:
                    raise Hold('private channel identity lost; refuse foreign close')
                os.close(fd)
                self._fds.pop(fd)
            if self.client is not None:
                await self.client.aclose()
            for stream in (self.child.stdout, self.child.stderr) if self.child is not None else ():
                stream.close()
            if self.root is not None:
                self._storage()
                shutil.rmtree(self.root)
            if hasattr(self, 'log'):
                self.log.close()
            self._credential_live = False
            self._active = None
            self._closed = True
            self._record('DISPOSED', {'redis_pid': self.process.pid if self.process else None,
                'storage_removed': self.root is None or not self.root.exists(),
                'credential_invalidated_by_exit': self.process is not None and self.process.poll() is not None,
                'errors': errors})
            if errors:
                self._cleanup_error = True
                raise Hold('verified disposal with credential cleanup error: HOLD')
        except BaseException as exc:
            self._cleanup_error = True
            self._record('CLEANUP_HOLD', {'error_type': type(exc).__name__,
                'unverified_signal_reset_or_delete_permitted': False})
            raise Hold('legacy cleanup failed: HOLD') from None
        finally:
            for fd in (redis_fd, child_fd):
                if fd is not None:
                    os.close(fd)


def _verify_binding(hello):
    """Child-side external corroboration, never treats a JSON record as a grant."""
    parent = int(next(l.split()[1] for l in Path('/proc/self/status').read_text().splitlines()
                      if l.startswith('PPid:')))
    if (_host(Path(f'/proc/{parent}')) != hello['issuer'] or file_digest(SOURCE) != hello['source_sha256']
            or time.monotonic() >= hello['deadline']):
        raise Hold('original live legacy issuer/source/deadline lost')
    b = hello['binding']
    if b['kind'] != 'OWNED_LEGACY' or b['db'] != 0:
        raise Hold('not a legacy DB0 creation binding')
    h = b['host']
    current = _host(Path(f'/proc/{h["proc_pid"]}'))
    status = dict(l.split(':', 1) for l in Path(f'/proc/{h["proc_pid"]}/status').read_text().splitlines())
    if (any(current[k] != h[k] for k in ('pid', 'proc_pid', 'start_ticks', 'exe', 'exe_identity'))
            or current['namespace'] != h['pid_namespace']
            or int(status['PPid']) != hello['issuer']['proc_pid']):
        raise Hold('foreign legacy Redis process')
    storage = Path(b['storage'])
    _private(storage, 0o700)
    if (str(Path(f'/proc/{h["proc_pid"]}/cwd').resolve()) != str(storage)
            or list(_identity(storage)) != b['storage_identity']
            or file_digest(Path(b['config'])) != b['config_sha256']
            or file_digest(Path(b['acl'])) != b['acl_sha256']
            or file_digest(Path(h['exe'])) != b['binary_sha256']
            or listener_evidence(h['proc_pid'], b['listener']['port']) != b['listener']):
        raise Hold('legacy listener/storage/binary provenance mismatch')


async def _child_handoff(read_fd, write_fd):
    _pipe(read_fd, write=False)
    _pipe(write_fd, write=True)
    hello = await _receive(read_fd, time.monotonic() + 15)
    if (hello.get('kind') != 'LEGACY_CHALLENGE' or hello.get('child') != _host(Path('/proc/self'))
            or hello.get('arguments') != sys.argv[1:]
            or hello.get('read_inode') != os.fstat(read_fd).st_ino
            or hello.get('write_inode') != os.fstat(write_fd).st_ino):
        raise Hold('invalid original pytest child or channel')
    _verify_binding(hello)
    challenge = secrets.token_hex(32)
    await _send(write_fd, {'kind': 'LEGACY_RESPONSE', 'nonce': hello['nonce'], 'child': hello['child'],
        'source_sha256': file_digest(SOURCE), 'challenge': challenge}, hello['deadline'])
    credential = await _receive(read_fd, hello['deadline'])
    if (credential.get('kind') != 'LEGACY_CREDENTIAL' or credential.get('nonce') != hello['nonce']
            or credential.get('challenge') != challenge or credential.get('username') != 'legacy_fixture'
            or not re.fullmatch('[0-9a-f]{64}', credential.get('password', ''))):
        raise Hold('missing/wrong/prepared credential at legacy boundary')
    _verify_binding(hello)
    client = redis.Redis(host='127.0.0.1', port=hello['binding']['listener']['port'], db=0,
        username='legacy_fixture', password=credential['password'], decode_responses=True,
        socket_connect_timeout=2, socket_timeout=2)
    try:
        info = await client.info('server')
        if info['process_id'] != hello['binding']['host']['pid'] or info['run_id'] != hello['binding']['run_id']:
            raise Hold('credential connected to wrong legacy domain')
    except Exception:
        raise Hold('legacy credential corroboration failed') from None
    finally:
        await client.aclose()
    await _send(write_fd, {'kind': 'LEGACY_READY', 'nonce': hello['nonce']}, hello['deadline'])
    return hello, credential


class _LegacyAdapter:
    """Authenticate only fresh clients to the exact original legacy endpoint.

    Shared fixtures and callers keep their functions, URLs, decode mode and test
    identities. Prepared clients at other listeners retain their own credentials.
    Unauthorized FLUSHDB (including pipeline) is refused before connecting.
    """
    def __init__(self, hello, credential):
        self.hello, self.credential = hello, credential
        self.pools = weakref.WeakSet()
        self.events, self.phases, self.nodes, self.origins = [], [], [], []
        self.original_init = redis.Redis.__init__
        self.original_execute = redis.Redis.execute_command
        self.original_pipeline = redis.client.Pipeline.execute

    def verify(self):
        _verify_binding(self.hello)

    def record(self, kind, value):
        self.events.append({'kind': kind, 'value': value})

    def install(self):
        adapter = self
        def init(client, *args, **kwargs):
            pool = kwargs.get('connection_pool')
            k = pool.connection_kwargs if pool is not None else kwargs
            target = (k.get('host', 'localhost'), int(k.get('port', 6379)))
            if target == ('127.0.0.1', adapter.hello['binding']['listener']['port']):
                adapter.verify()
                if (int(k.get('db', 0)) != 0 or any(k.get(n) is not None for n in
                        ('username', 'password', 'credential_provider'))):
                    raise Hold('foreign/prepared/caller credential or DB at legacy boundary')
                if pool is not None and (pool._available_connections or pool._in_use_connections):
                    raise Hold('no adoption of an already connected caller pool')
                k.update(username='legacy_fixture', password=adapter.credential['password'])
            adapter.original_init(client, *args, **kwargs)
            k = client.connection_pool.connection_kwargs
            if target == ('127.0.0.1', adapter.hello['binding']['listener']['port']):
                adapter.pools.add(client.connection_pool)
                adapter.record('LEGACY_CLIENT', {'endpoint': adapter.hello['binding']['endpoint'],
                    'node': os.environ.get('PYTEST_CURRENT_TEST'), 'decode_responses': k.get('decode_responses', False)})
        async def execute(client, *args, **kwargs):
            command = args[0].decode() if isinstance(args[0], bytes) else str(args[0])
            if command.upper() in ('FLUSHDB', 'FLUSHALL') or client.connection_pool in adapter.pools:
                await adapter.guard(client.connection_pool, command.upper(),
                    lambda *a: adapter.original_execute(client, *a))
            result = await adapter.original_execute(client, *args, **kwargs)
            if command.upper() == 'FLUSHDB':
                adapter.record('FLUSHDB_COMPLETED', {'endpoint': adapter.hello['binding']['endpoint'],
                    'run_id': adapter.hello['binding']['run_id'], 'pid': adapter.hello['binding']['host']['pid'],
                    'node': os.environ.get('PYTEST_CURRENT_TEST'), 'result': bool(result)})
            return result
        async def pipeline(client, *args, **kwargs):
            commands = [(a[0][0].decode() if isinstance(a[0][0], bytes) else str(a[0][0])).upper()
                        for a in client.command_stack]
            if any(c in ('FLUSHDB', 'FLUSHALL') for c in commands):
                # Independent direct client with the *same registered* pool; no new credentials.
                probe = object.__new__(redis.Redis)
                adapter.original_init(probe, connection_pool=client.connection_pool)
                try:
                    for c in commands:
                        if c in ('FLUSHDB', 'FLUSHALL'):
                            await adapter.guard(client.connection_pool, c,
                                lambda *a: adapter.original_execute(probe, *a))
                finally:
                    await probe.aclose()
            result = await adapter.original_pipeline(client, *args, **kwargs)
            if 'FLUSHDB' in commands:
                adapter.record('PIPELINE_FLUSHDB_COMPLETED', {'endpoint': adapter.hello['binding']['endpoint'],
                    'run_id': adapter.hello['binding']['run_id'], 'count': commands.count('FLUSHDB')})
            return result
        redis.Redis.__init__, redis.Redis.execute_command = init, execute
        redis.client.Pipeline.execute = pipeline

    async def guard(self, pool, command, execute):
        self.verify()
        k = pool.connection_kwargs
        b = self.hello['binding']
        if (pool not in self.pools or (k.get('host'), int(k.get('port', 6379)), int(k.get('db', 0)))
                != ('127.0.0.1', b['listener']['port'], 0)
                or k.get('username') != 'legacy_fixture' or k.get('password') != self.credential['password']):
            raise Hold('unverified FLUSHDB/client target; no connection')
        if command == 'FLUSHALL':
            raise Hold('FLUSHALL is not a legacy fixture capability')
        if command == 'FLUSHDB':
            info = await execute('INFO', 'server')
            if info['process_id'] != b['host']['pid'] or info['run_id'] != b['run_id']:
                raise Hold('FLUSHDB target changed')
            if await execute('EXISTS', HEADER, OWNERS):
                raise Hold('prepared/retained owner evidence forbids legacy reset')

    def restore(self):
        redis.Redis.__init__, redis.Redis.execute_command = self.original_init, self.original_execute
        redis.client.Pipeline.execute = self.original_pipeline

    def write(self):
        raw = json.dumps({'nodes': self.nodes, 'phases': self.phases,
            'events': self.events, 'fixture_origins': self.origins}, sort_keys=True, indent=2)
        if self.credential['password'] in raw:
            raise Hold('credential forbidden in public adapter evidence')
        # Path is within original creator-owned private storage; copied JSON cannot choose it.
        target = Path(self.hello['binding']['storage'])/'pytest-observation.json'
        target.write_text(raw + '\n')


_ADAPTER = None


def legacy_endpoint():
    if _ADAPTER is None:
        raise Hold('live inherited legacy handoff required')
    _ADAPTER.verify()
    return _ADAPTER.hello['binding']['endpoint']


def pytest_configure(config):
    global _ADAPTER
    read, write = os.environ.pop(READ_FD, None), os.environ.pop(WRITE_FD, None)
    if read is None or write is None or _ADAPTER is not None:
        raise Hold('missing original private legacy channels')
    try:
        read, write = int(read), int(write)
        hello, credential = asyncio.run(_child_handoff(read, write))
    finally:
        # Close only descriptor ends whose type/direction was independently checked.
        for fd, writing in ((read, False), (write, True)):
            if type(fd) is int:
                _pipe(fd, write=writing)
                os.close(fd)
    _ADAPTER = _LegacyAdapter(hello, credential)
    _ADAPTER.install()


def pytest_collection_finish(session):
    import inspect
    _ADAPTER.nodes = [i.nodeid for i in session.items]
    for item in session.items:
        for name in ('integration_redis_stack', 'strict_mode_env', 'flush_redis'):
            definitions = item._fixtureinfo.name2fixturedefs.get(name)
            if definitions:
                _ADAPTER.origins.append({'node': item.nodeid, 'fixture': name,
                    'source': inspect.getsourcefile(definitions[-1].func)})


def _redact_report(report):
    # Redact failure/capture text only; never change outcomes or test identities.
    secret = _ADAPTER.credential['password']
    if report.failed and secret in str(report.longrepr):
        report.longrepr = str(report.longrepr).replace(secret, '<redacted>')
    report.sections = [(name, text.replace(secret, '<redacted>')) for name, text in report.sections]


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    _redact_report(outcome.get_result())


def pytest_runtest_logreport(report):
    _redact_report(report)
    _ADAPTER.phases.append({'node': report.nodeid, 'when': report.when, 'outcome': report.outcome,
                            'wasxfail': getattr(report, 'wasxfail', None)})


def pytest_sessionfinish(session, exitstatus):
    if _ADAPTER is not None:
        _ADAPTER.record('PYTEST_FINISH', {'exitstatus': int(exitstatus)})
        _ADAPTER.write()


def pytest_unconfigure(config):
    global _ADAPTER
    if _ADAPTER is not None:
        _ADAPTER.restore()
        _ADAPTER = None


def main():
    parser = argparse.ArgumentParser(description='Owned legacy CI Redis; no prepared activation')
    parser.add_argument('--redis-binary', required=True, type=Path)
    parser.add_argument('--reviewed-sha256', required=True)
    parser.add_argument('--evidence-dir', required=True, type=Path)
    parser.add_argument('--timeout', type=float, default=300)
    parser.add_argument('pytest_arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    async def run():
        domain = OwnedLegacyEnvironment(args.redis_binary, expected_sha256=args.reviewed_sha256,
                                       evidence_parent=args.evidence_dir)
        try:
            await domain.create()
            argv = args.pytest_arguments
            if argv[:1] == ['--']:
                argv = argv[1:]
            result = await domain.run_pytest(argv, timeout=args.timeout)
            sys.stdout.write(result.stdout)
            sys.stderr.write(result.stderr)
            return result.returncode
        finally:
            if not domain._closed:
                await domain.close()
    return asyncio.run(run())


if __name__ == '__main__':
    raise SystemExit(main())

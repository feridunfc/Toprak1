#!/usr/bin/env python
"""Deterministic authority mutation audit for IRONCLAD / Toprak1.

Read-only by design:
- imports no project runtime modules,
- opens no Redis/network connections,
- writes no files,
- performs AST/static source analysis only.

Exit codes:
    0  no configured failing findings
    1  findings at/above configured failure threshold
    2  script/configuration error
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Literal

Severity = Literal["allowed", "suspicious", "banned"]
SuspiciousClass = Literal[
    "not_applicable",
    "governance_local",
    "observability_only",
    "lua_atomic_boundary",
    "lease_or_fencing",
    "rate_limit_or_admission",
    "recovery_or_reconciliation",
    "worker_or_effect_telemetry",
    "false_positive_static",
    "true_authority_review",
]

MUTATION_CALLS = {
    "set", "hset", "delete", "xadd", "zadd", "zrem", "sadd", "lpush", "rpush",
    "hincrby", "hincrbyfloat", "incr", "decr", "expire", "persist", "rename",
    "eval", "evalsha", "execute_command",
}

PIPELINE_COMMIT_CALLS = {"execute", "exec", "transaction"}

SAFE_AUTHORITY_TOKENS = {
    "transition_state",
    "AuthoritativeEventGate",
    "append_before_authoritative_write",
    "emit_event_background",
    "append_event",
    "EventStore",
    "LuaScriptLoader",
    "EffectLedger",
    "reconcile_tenant",

    # === Sprint 9 Reviewed Authority Markers ===
    "AUTHORITY_REVIEWED_PROJECTION_WRITE",
    "AUTHORITY_REVIEWED_NON_TRUTH_STATE",
    "AUTHORITY_REVIEWED_GOVERNANCE_STATE",
    "AUTHORITY_REVIEWED_LUA_ATOMIC_BOUNDARY",
    "AUTHORITY_REVIEWED_LEASE_COUNTER",
}

DEFAULT_SCAN_DIRS = (
    "hfa-core/src",
    "hfa-control/src",
    "hfa-worker/src",
    "hfa-agents/src",
    "hfa-semantic/src",
)

DEFAULT_LUA_DIRS = (
    "hfa-core/src/hfa/lua",
    "lua",
)

ALLOWED_AUTHORITY_FILES = {
    "hfa-core/src/hfa/state/__init__.py",
    "hfa-control/src/hfa_control/state_machine.py",
}

CRITICAL_RUNTIME_FILES = {
    "hfa-core/src/hfa/runtime/state_store.py",
    "hfa-control/src/hfa_control/scheduler_loop.py",
    "hfa-worker/src/hfa_worker/runtime/worker_runtime.py",
}

STATE_KEY_HINTS = (
    "run_state", "task_state", "state_key", "status_key", "status", "run_status",
    "task_status", "lifecycle", "terminal", "runtime_state", "current_state",
    "RedisKey.run_state", "DagRedisKey.task_state", "run:state", "task:state",
    "hfa:run", "hfa:task", ":state", ":status",
)

EVENT_APPEND_HINTS = (
    "xadd", "append_event", "append_before_authoritative_write", "emit_event_background",
)

STATE_LITERAL_VALUES = {
    "queued", "admitted", "scheduled", "running", "done", "failed", "rejected",
    "dead_lettered", "rescheduled", "completed", "quarantined", "blocked",
    "ready", "claimed", "cancelled", "succeeded", "error",
}

LUA_MUTATION_PATTERN = re.compile(
    r"redis\.call\s*\(\s*['\"](?P<cmd>[a-zA-Z]+)['\"](?P<rest>[^)]*)\)",
    re.IGNORECASE,
)

LUA_MUTATING_COMMANDS = {
    "set", "hset", "del", "delete", "xadd", "zadd", "zrem", "sadd", "lpush", "rpush",
    "hincrby", "incr", "decr", "expire",
}


@dataclass(frozen=True)
class AuditFinding:
    severity: Severity
    path: str
    line: int
    call: str
    category: str
    reason: str
    context: str
    function: str = "<module>"
    suspicious_class: SuspiciousClass = "not_applicable"
    risk_score: int = 0


@dataclass(frozen=True)
class AuditSummary:
    allowed: int
    suspicious: int
    banned: int
    scanned_files: int
    findings: list[AuditFinding]

    @property
    def status(self) -> str:
        if self.banned:
            return "FAIL"
        if self.suspicious:
            return "PASS_WITH_RISKS"
        return "PASS"


class AuthorityVisitor(ast.NodeVisitor):
    def __init__(self, *, path: Path, repo_root: Path, source: str) -> None:
        self.path = path
        self.repo_root = repo_root
        self.rel_path = path.relative_to(repo_root).as_posix()
        self.source = source
        self.findings: list[AuditFinding] = []
        self.assignments: dict[str, str] = {}
        self.imported_names: set[str] = set()
        self._function_stack: list[str] = []

    @property
    def current_function(self) -> str:
        return ".".join(self._function_stack) if self._function_stack else "<module>"

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self.imported_names.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.imported_names.add(alias.asname or alias.name.split(".")[0])
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        value_repr = _safe_unparse(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.assignments[target.id] = value_repr
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name) and node.value is not None:
            self.assignments[node.target.id] = _safe_unparse(node.value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func_name = _call_name(node)
        attr_name = _call_attr(node)
        if attr_name in MUTATION_CALLS:
            self._record_mutation(node, attr_name, func_name)
        elif attr_name in PIPELINE_COMMIT_CALLS and _looks_like_pipeline_commit(node):
            self._record_pipeline_commit(node, attr_name, func_name)
        self.generic_visit(node)

    def _finding(
        self,
        *,
        severity: Severity,
        line: int,
        call: str,
        category: str,
        reason: str,
        context: str,
    ) -> AuditFinding:
        suspicious_class = _classify_suspicious(
            severity=severity,
            path=self.rel_path,
            function=self.current_function,
            category=category,
            call=call,
            context=context,
            reason=reason,
        )
        return AuditFinding(
            severity=severity,
            path=self.rel_path,
            line=line,
            call=call,
            category=category,
            reason=reason,
            context=context,
            function=self.current_function,
            suspicious_class=suspicious_class,
            risk_score=_risk_score(
                severity,
                self.rel_path in CRITICAL_RUNTIME_FILES,
                suspicious_class,
            ),
        )

    def _record_pipeline_commit(self, node: ast.Call, attr_name: str, func_name: str) -> None:
        severity: Severity = "banned" if self.rel_path in CRITICAL_RUNTIME_FILES else "suspicious"
        self.findings.append(
            self._finding(
                severity=severity,
                line=getattr(node, "lineno", 0),
                call=func_name,
                category="redis_pipeline_commit",
                reason="pipeline commit requires review for event-gated authority semantics",
                context=_source_line(self.source, getattr(node, "lineno", 0)),
            )
        )

    def _record_mutation(self, node: ast.Call, attr_name: str, func_name: str) -> None:
        lineno = getattr(node, "lineno", 0)
        call_repr = _safe_unparse(node)
        key_repr = _safe_unparse(node.args[0]) if node.args else ""
        value_repr = _safe_unparse(node.args[1]) if len(node.args) > 1 else ""
        surrounding = _surrounding_source(self.source, lineno, window=8)
        category = _categorize_mutation(attr_name, key_repr, value_repr, call_repr)
        severity, reason = self._classify(
            attr_name=attr_name,
            category=category,
            key_repr=key_repr,
            value_repr=value_repr,
            call_repr=call_repr,
            surrounding=surrounding,
        )
        self.findings.append(
            self._finding(
                severity=severity,
                line=lineno,
                call=func_name,
                category=category,
                reason=reason,
                context=_source_line(self.source, lineno),
            )
        )

    def _classify(
        self,
        *,
        attr_name: str,
        category: str,
        key_repr: str,
        value_repr: str,
        call_repr: str,
        surrounding: str,
    ) -> tuple[Severity, str]:
        if self.rel_path in ALLOWED_AUTHORITY_FILES:
            return "allowed", "canonical authority/compatibility file"

        if category == "event_append":
            return "allowed", "event append is observational/authority log path"

        # has_safe_authority = any(token in surrounding for token in SAFE_AUTHORITY_TOKENS)
        # Mevcut satırın yerine şunu koy:
        # surrounding = _surrounding_source(self.source, lineno, window=12)  # 8 → 12 yap
        # has_safe_authority = any(token in surrounding for token in SAFE_AUTHORITY_TOKENS)
        has_safe_authority = any(token in surrounding for token in SAFE_AUTHORITY_TOKENS)
        is_critical = self.rel_path in CRITICAL_RUNTIME_FILES
        stateish = _looks_stateish(key_repr, value_repr, call_repr)

        if is_critical:
            if has_safe_authority:
                return "allowed", "critical runtime mutation adjacent to safe authority token"
            return "banned", "critical runtime mutation without safe authority token"

        if stateish and attr_name in {"set", "hset", "delete"}:
            if not has_safe_authority:
                return "banned", "state-like direct mutation without nearby authority/event gate"
            return "suspicious", "state-like mutation near authority helper; review required"

        if attr_name in {"eval", "evalsha"}:
            return "suspicious", "Lua mutation boundary must be reviewed for invariant coverage"

        return "suspicious", "Redis/write-like mutation candidate; review authority semantics"




def _classify_suspicious(
    *,
    severity: Severity,
    path: str,
    function: str,
    category: str,
    call: str,
    context: str,
    reason: str,
) -> SuspiciousClass:
    """Assign a review-oriented class to suspicious findings.

    This does not lower severity and does not mark findings as allowed. It only
    makes the remaining review backlog more readable for dashboards and planning.
    """
    if severity != "suspicious":
        return "not_applicable"

    lowered = " ".join((path, function, category, call, context, reason)).lower()

    static_set_call = (
        call == "set"
        or context.strip().startswith("set(")
        or " set(" in f" {context.lower()}"
        or "= set()" in lowered
        or "= set(" in lowered
        or ": set[" in lowered
        or ".setdefault(" in lowered
        or "caps = set(" in lowered
        or "merged: set" in lowered
        or "set()" in lowered
    )
    if static_set_call and not any(
        token in lowered
        for token in ("redis.", "self._redis", "await redis", "await self._redis", "pipe.")
    ):
        return "false_positive_static"


    if (
        " set(" in f" {context.lower()}"
        or "= set()" in lowered
        or ": set[" in lowered
        or ".setdefault(" in lowered
        or "caps = set(" in lowered
        or "merged: set" in lowered
    ) and "redis" not in lowered and "_redis" not in lowered:
        return "false_positive_static"

    if "lua" in path or "evalsha" in lowered or "eval(" in lowered or category in {"lua_mutation", "lua_script_mutation"}:
        return "lua_atomic_boundary"

    if any(token in lowered for token in ("budget", "governance", "ledger", "signed_ledger")):
        return "governance_local"

    if any(token in lowered for token in ("obs/", "audit_store", "decision_trace", "metrics", "prometheus", "graph_store", "run_graph", "operational_metrics")):
        return "observability_only"

    if any(token in lowered for token in ("leader", "shard", "reservation", "idempotency", "token", "owner", "ttl", "fence", "heartbeat")):
        return "lease_or_fencing"

    if any(token in lowered for token in ("rate_limit", "admission", "tenant_registry", "tenant_queue", "fairness", "inflight", "vruntime")):
        return "rate_limit_or_admission"

    if any(token in lowered for token in ("recovery", "reconcile", "reconciliation", "failure_sweeper", "healing", "dead_letter", "dlq")):
        return "recovery_or_reconciliation"

    if any(token in lowered for token in ("worker", "effect_ledger", "effect", "drain", "consumer")):
        return "worker_or_effect_telemetry"

    return "true_authority_review"


def _call_attr(node: ast.Call) -> str:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return ""


def _call_name(node: ast.Call) -> str:
    return _safe_unparse(node.func)


def _safe_unparse(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return type(node).__name__


def _source_line(source: str, lineno: int) -> str:
    lines = source.splitlines()
    if 1 <= lineno <= len(lines):
        return lines[lineno - 1].strip()
    return ""


def _surrounding_source(source: str, lineno: int, *, window: int) -> str:
    lines = source.splitlines()
    start = max(0, lineno - window - 1)
    end = min(len(lines), lineno + window)
    return "\n".join(lines[start:end])


def _looks_stateish(key_repr: str, value_repr: str, call_repr: str) -> bool:
    joined = " ".join((key_repr, value_repr, call_repr)).lower()
    normalized = re.sub(r"[^a-z0-9_:./]+", "_", joined)
    if any(hint.lower() in joined or hint.lower() in normalized for hint in STATE_KEY_HINTS):
        return True
    cleaned_value = value_repr.strip("'\"").lower()
    if cleaned_value in STATE_LITERAL_VALUES:
        return True
    return any(f"'{state}'" in joined or f'"{state}"' in joined for state in STATE_LITERAL_VALUES)


def _categorize_mutation(attr_name: str, key_repr: str, value_repr: str, call_repr: str) -> str:
    if attr_name == "xadd" or any(hint in call_repr for hint in EVENT_APPEND_HINTS):
        return "event_append"
    if _looks_stateish(key_repr, value_repr, call_repr):
        return "state_mutation"
    if attr_name in {"hset", "set", "delete"}:
        return "redis_mutation"
    if attr_name in {"zadd", "zrem", "sadd", "lpush", "rpush"}:
        return "queue_or_index_mutation"
    if attr_name in {"eval", "evalsha"}:
        return "lua_mutation"
    return "mutation"


def _looks_like_pipeline_commit(node: ast.Call) -> bool:
    func = node.func
    if not isinstance(func, ast.Attribute):
        return False
    target = _safe_unparse(func.value).lower()
    return any(token in target for token in ("pipe", "pipeline", "txn", "transaction"))


def _risk_score(
    severity: Severity,
    critical: bool = False,
    suspicious_class: SuspiciousClass = "not_applicable",
) -> int:
    if severity == "suspicious" and suspicious_class == "false_positive_static":
        return 0
    if severity == "suspicious" and suspicious_class == "observability_only":
        return 1
    base = {"allowed": 0, "suspicious": 5, "banned": 100}[severity]
    return base + (25 if critical and severity != "allowed" else 0)


def iter_python_files(repo_root: Path, scan_dirs: Iterable[str]) -> Iterable[Path]:
    seen: set[Path] = set()
    for rel in scan_dirs:
        directory = repo_root / rel
        if not directory.exists():
            continue
        for path in directory.rglob("*.py"):
            if path in seen:
                continue
            if "__pycache__" not in path.parts and ".egg-info" not in path.as_posix():
                seen.add(path)
                yield path


def iter_lua_files(repo_root: Path, lua_dirs: Iterable[str] = DEFAULT_LUA_DIRS) -> Iterable[Path]:
    seen: set[Path] = set()
    for rel in lua_dirs:
        directory = repo_root / rel
        if not directory.exists():
            continue
        for path in directory.rglob("*.lua"):
            if path not in seen:
                seen.add(path)
                yield path


def scan_lua_file(path: Path, repo_root: Path) -> list[AuditFinding]:
    rel = path.relative_to(repo_root).as_posix()
    findings: list[AuditFinding] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError as exc:
        return [
            AuditFinding("banned", rel, 0, "read", "lua", f"could not read lua file: {exc}", "", risk_score=100)
        ]
    for idx, line in enumerate(lines, start=1):
        match = LUA_MUTATION_PATTERN.search(line)
        if not match:
            continue
        cmd = match.group("cmd").lower()
        if cmd not in LUA_MUTATING_COMMANDS:
            continue
        call = f"redis.call('{cmd}', ...)"
        stateish = _looks_stateish(line, line, line)
        if cmd == "xadd":
            severity: Severity = "allowed"
            reason = "Lua event append path"
        elif stateish:
            severity = "suspicious"
            reason = "Lua state-like mutation; invariant coverage required"
        else:
            severity = "suspicious"
            reason = "Lua write command; review atomic authority invariants"
        suspicious_class = _classify_suspicious(
            severity=severity,
            path=rel,
            function="<lua>",
            category="lua_script_mutation",
            call=call,
            context=line.strip(),
            reason=reason,
        )
        findings.append(
            AuditFinding(
                severity=severity,
                path=rel,
                line=idx,
                call=call,
                category="lua_script_mutation",
                reason=reason,
                context=line.strip(),
                function="<lua>",
                suspicious_class=suspicious_class,
                risk_score=_risk_score(severity, suspicious_class=suspicious_class),
            )
        )
    return findings


def run_audit(
    repo_root: Path,
    *,
    scan_dirs: Iterable[str] = DEFAULT_SCAN_DIRS,
    include_lua: bool = True,
) -> AuditSummary:
    findings: list[AuditFinding] = []
    scanned = 0
    for path in iter_python_files(repo_root, scan_dirs):
        scanned += 1
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
        except (SyntaxError, UnicodeDecodeError) as exc:
            findings.append(
                AuditFinding(
                    severity="banned",
                    path=path.relative_to(repo_root).as_posix(),
                    line=0,
                    call="parse",
                    category="syntax",
                    reason=f"file cannot be parsed: {exc}",
                    context="",
                    risk_score=100,
                )
            )
            continue
        visitor = AuthorityVisitor(path=path, repo_root=repo_root, source=source)
        visitor.visit(tree)
        findings.extend(visitor.findings)

    if include_lua:
        for path in iter_lua_files(repo_root):
            scanned += 1
            findings.extend(scan_lua_file(path, repo_root))

    return AuditSummary(
        allowed=sum(1 for f in findings if f.severity == "allowed"),
        suspicious=sum(1 for f in findings if f.severity == "suspicious"),
        banned=sum(1 for f in findings if f.severity == "banned"),
        scanned_files=scanned,
        findings=findings,
    )


def heatmap(summary: AuditSummary) -> list[dict[str, object]]:
    files: dict[str, dict[str, object]] = {}
    for finding in summary.findings:
        entry = files.setdefault(
            finding.path,
            {
                "path": finding.path,
                "allowed": 0,
                "suspicious": 0,
                "banned": 0,
                "risk_score": 0,
                "class_counts": {},
            },
        )
        entry[finding.severity] = int(entry[finding.severity]) + 1
        entry["risk_score"] = int(entry["risk_score"]) + finding.risk_score
        class_counts = entry.setdefault("class_counts", {})
        if finding.severity == "suspicious":
            class_counts[finding.suspicious_class] = int(class_counts.get(finding.suspicious_class, 0)) + 1
    return sorted(files.values(), key=lambda item: int(item["risk_score"]), reverse=True)


def suspicious_class_counts(summary: AuditSummary) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in summary.findings:
        if finding.severity != "suspicious":
            continue
        counts[finding.suspicious_class] = counts.get(finding.suspicious_class, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def noise_count(summary: AuditSummary) -> int:
    return sum(
        1
        for finding in summary.findings
        if finding.severity == "suspicious"
        and finding.suspicious_class == "false_positive_static"
    )


def risk_bearing_suspicious_count(summary: AuditSummary) -> int:
    return sum(
        1
        for finding in summary.findings
        if finding.severity == "suspicious"
        and finding.suspicious_class not in {"false_positive_static", "observability_only"}
    )


def telemetry_risk_count(summary: AuditSummary) -> int:
    return sum(
        1
        for finding in summary.findings
        if finding.severity == "suspicious"
        and finding.suspicious_class == "observability_only"
    )


def summary_to_dashboard(summary: AuditSummary) -> dict[str, object]:
    return {
        "authority_status": summary.status,
        "risk_score": sum(f.risk_score for f in summary.findings),
        "noise_count": noise_count(summary),
        "telemetry_risk_count": telemetry_risk_count(summary),
        "risk_bearing_suspicious": risk_bearing_suspicious_count(summary),
        "suspicious_classes": suspicious_class_counts(summary),
        "counts": {
            "allowed": summary.allowed,
            "suspicious": summary.suspicious,
            "banned": summary.banned,
            "scanned_files": summary.scanned_files,
        },
        "critical_files": sorted(CRITICAL_RUNTIME_FILES),
        "heatmap": heatmap(summary),
        "findings": [asdict(f) for f in summary.findings],
    }


def emit_text(summary: AuditSummary, *, max_findings: int) -> str:
    lines = [
        f"Authority audit status: {summary.status}",
        f"Scanned files: {summary.scanned_files}",
        f"Allowed: {summary.allowed}  Suspicious: {summary.suspicious}  Banned: {summary.banned}",
        f"Risk score: {sum(f.risk_score for f in summary.findings)}",
    ]
    if summary.findings:
        lines.extend(["", "Findings:"])
        for finding in summary.findings[:max_findings]:
            lines.append(
                f"- [{finding.severity.upper()}] {finding.path}:{finding.line} "
                f"{finding.function} {finding.category} {finding.call} "
                f"class={finding.suspicious_class} :: {finding.reason}"
            )
        if len(summary.findings) > max_findings:
            lines.append(f"... {len(summary.findings) - max_findings} more findings")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only authority mutation audit.")
    parser.add_argument("--repo-root", default=".", help="Repository root path.")
    parser.add_argument("--format", choices=("text", "json", "dashboard"), default="text")
    parser.add_argument("--heatmap", action="store_true", help="Emit file-level risk heatmap JSON and exit.")
    parser.add_argument("--max-findings", type=int, default=80)
    parser.add_argument(
        "--fail-on",
        choices=("banned", "suspicious", "none"),
        default="banned",
        help="Which finding severity should make the process exit non-zero.",
    )
    parser.add_argument("--strict", action="store_true", help="Alias for --fail-on suspicious.")
    parser.add_argument("--no-lua", action="store_true", help="Disable Lua script scanning.")
    parser.add_argument("--include", action="append", default=[], help="Additional directory to scan.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    if not repo_root.exists():
        print(f"repo root not found: {repo_root}", file=sys.stderr)
        return 2
    scan_dirs = tuple(DEFAULT_SCAN_DIRS) + tuple(args.include)
    summary = run_audit(repo_root, scan_dirs=scan_dirs, include_lua=not args.no_lua)

    if args.heatmap:
        print(json.dumps(heatmap(summary), indent=2, sort_keys=True))
    elif args.format == "json":
        print(json.dumps(asdict(summary), indent=2, sort_keys=True))
    elif args.format == "dashboard":
        print(json.dumps(summary_to_dashboard(summary), indent=2, sort_keys=True))
    else:
        print(emit_text(summary, max_findings=args.max_findings))

    fail_on = "suspicious" if args.strict else args.fail_on
    if fail_on == "none":
        return 0
    if fail_on == "suspicious" and (summary.suspicious or summary.banned):
        return 1
    if fail_on == "banned" and summary.banned:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



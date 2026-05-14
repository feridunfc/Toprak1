"""
tests/core/test_no_direct_run_state_writes.py
IRONCLAD Sprint 22 — Architectural invariant: no direct run_state writes

AST-based analysis that catches .set() calls where the first argument
traces to a run_state key — including pipe.set(state_key, ...),
redis.set(some_var, "queued", ...) etc.

All state writes MUST go through hfa.state.transition_state().

Allowed exceptions:
  hfa/state/__init__.py  — the write authority itself (contains the one true SET)
  state_machine.py       — compat re-export shim (no direct writes)
"""
import ast
import re
from pathlib import Path


ALLOWED_FILES = {"__init__.py", "state_machine.py"}

STATE_KEY_NAMES = re.compile(
    r"state_key|run_state|STATE_KEY",
    re.IGNORECASE,
)

STATE_VALUES = frozenset({
    "queued", "admitted", "scheduled", "running",
    "done", "failed", "rejected", "dead_lettered", "rescheduled",
})


class DirectStateWriteFinder(ast.NodeVisitor):
    def __init__(self, filename: str):
        self.filename = filename
        self.violations: list[tuple[int, str]] = []
        self._assignments: dict[str, str] = {}

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            if isinstance(target, ast.Name):
                self._assignments[target.id] = ast.unparse(node.value)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "set":
            if node.args:
                self._check_set_call(node)
        self.generic_visit(node)

    def _check_set_call(self, node: ast.Call) -> None:
        key_arg = node.args[0]
        key_repr = ast.unparse(key_arg)
        lineno = getattr(node, "lineno", 0)

        # Pattern 1: literal key contains run_state / STATE_KEY
        if any(p in key_repr for p in ["run_state", "STATE_KEY", "run:state:"]):
            val = ast.unparse(node.args[1]) if len(node.args) > 1 else "?"
            self.violations.append((lineno, f".set({key_repr}, {val})"))
            return

        # Pattern 2: variable whose name looks like a state key
        if isinstance(key_arg, ast.Name):
            vname = key_arg.id
            assigned = self._assignments.get(vname, "")
            if STATE_KEY_NAMES.search(vname) or STATE_KEY_NAMES.search(assigned):
                # Only flag when the value is a known state literal
                val_repr = ast.unparse(node.args[1]) if len(node.args) > 1 else ""
                val_clean = val_repr.strip('"').strip("'")
                if val_clean in STATE_VALUES:
                    self.violations.append((
                        lineno,
                        f".set({vname!r}, {val_repr}) via state-key variable",
                    ))


def _source_files(base: Path):
    for pkg in ["hfa-control/src", "hfa-worker/src", "hfa-core/src"]:
        d = base / pkg
        if d.exists():
            for p in d.rglob("*.py"):
                if "__pycache__" not in str(p):
                    yield p


def test_no_direct_run_state_writes():
    """AST guard: every run_state write must go through hfa.state.transition_state."""
    repo = Path(__file__).parent.parent.parent
    violations = []

    for path in _source_files(repo):
        if path.name in ALLOWED_FILES:
            continue
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
        except (SyntaxError, UnicodeDecodeError):
            continue

        finder = DirectStateWriteFinder(str(path))
        finder.visit(tree)

        rel = path.relative_to(repo)
        for lineno, msg in finder.violations:
            violations.append(f"{rel}:{lineno}: {msg}")

    assert not violations, (
        "DIRECT RUN_STATE WRITES — route through hfa.state.transition_state():\n"
        + "\n".join(f"  {v}" for v in violations)
    )


def test_state_authority_in_hfa_core():
    """State machine authority must live in hfa-core/hfa/state, not hfa-control."""
    repo = Path(__file__).parent.parent.parent
    authority = repo / "hfa-core" / "src" / "hfa" / "state" / "__init__.py"
    assert authority.exists(), "hfa/state/__init__.py missing — authority must be in hfa-core"
    src = authority.read_text()
    assert "TransitionResult" in src
    assert "VALID_TRANSITIONS" in src
    assert "transition_state" in src


def test_state_machine_shim_re_exports():
    """hfa_control.state_machine must be a compat shim, not the authority."""
    repo = Path(__file__).parent.parent.parent
    shim = repo / "hfa-control" / "src" / "hfa_control" / "state_machine.py"
    src = shim.read_text()
    assert "from hfa.state import" in src, "shim must re-export from hfa.state"
    tree = ast.parse(src)
    fns = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "transition_state" not in fns, "shim must not define transition_state"
    assert "_python_transition" not in fns, "shim must not contain fallback logic"


def test_lua_state_transition_exists_with_prior():
    """state_transition.lua must exist and return {code, prior_state} table."""
    repo = Path(__file__).parent.parent.parent
    lua = repo / "hfa-core" / "src" / "hfa" / "lua" / "state_transition.lua"
    assert lua.exists(), "state_transition.lua missing"
    content = lua.read_text()
    # Must return table, not plain integer
    assert "{1," in content.replace(" ", "") or "return {1" in content
    assert "{-1," in content.replace(" ", "") or "return {-1" in content


def test_transition_state_imported_in_key_modules():
    """Every state-writing module must import transition_state."""
    repo = Path(__file__).parent.parent.parent
    required = [
        "hfa-control/src/hfa_control/scheduler.py",
        "hfa-control/src/hfa_control/admission.py",
        "hfa-control/src/hfa_control/recovery.py",
        "hfa-control/src/hfa_control/scheduler_loop.py",
        "hfa-core/src/hfa/runtime/state_store.py",
    ]
    missing = []
    for r in required:
        p = repo / r
        if p.exists() and "transition_state" not in p.read_text(encoding="utf-8", errors="ignore"):
            missing.append(r)
    assert not missing, "Missing transition_state:\n" + "\n".join(f"  {m}" for m in missing)


def test_all_source_files_parse():
    repo = Path(__file__).parent.parent.parent
    errors = []
    for path in _source_files(repo):
        try:
            ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as e:
            errors.append(f"{path.name}: {e}")
    assert not errors, "Syntax errors:\n" + "\n".join(errors)

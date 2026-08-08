from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _python_results_xadd_functions(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    parents: list[ast.AST] = []
    found: list[str] = []

    class Visitor(ast.NodeVisitor):
        def generic_visit(self, node):
            parents.append(node)
            super().generic_visit(node)
            parents.pop()

        def visit_Call(self, node: ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "xadd" and node.args:
                first = ast.get_source_segment(path.read_text(encoding="utf-8"), node.args[0]) or ""
                if "RedisKey.stream_results()" in first:
                    owner = next(
                        (
                            p.name
                            for p in reversed(parents)
                            if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef))
                        ),
                        "<module>",
                    )
                    found.append(owner)
            self.generic_visit(node)

    Visitor().visit(tree)
    return found


def test_python_terminal_results_writer_is_centralized_in_evidence_helper():
    consumer = ROOT / "hfa-worker/src/hfa_worker/consumer.py"
    assert _python_results_xadd_functions(consumer) == ["_append_terminal_event_with_evidence"]
    source = consumer.read_text(encoding="utf-8")
    assert source.count("_append_terminal_event_with_evidence(self._redis, evt)") == 5
    helper = source[source.index("async def _append_terminal_event_with_evidence"):source.index("async def _verify_run_requested_task_identity")]
    assert "pipe.xadd(RedisKey.stream_results()" in helper
    assert "pipe.hset(index_key, evidence_field, encoded)" in helper
    assert "pipe.multi()" in helper


def test_lua_terminal_producers_are_exactly_the_two_accounted_writers():
    lua_root = ROOT / "hfa-core/src/hfa/lua"
    terminal_writers = []
    for path in lua_root.glob("*.lua"):
        source = path.read_text(encoding="utf-8")
        if ("RunCompleted" in source or "RunFailed" in source) and '"XADD"' in source:
            terminal_writers.append(path.name)
            assert "terminal_event_index_key" in source
            assert "evidence_field" in source
    assert sorted(terminal_writers) == [
        "run_terminate_from_tasks.lua",
        "run_terminate_projection.lua",
    ]


def test_all_python_results_stream_xadd_calls_are_exactly_accounted():
    writers: list[tuple[str, str]] = []
    for root_name in ("hfa-control", "hfa-core", "hfa-worker"):
        for path in (ROOT / root_name / "src").rglob("*.py"):
            for owner in _python_results_xadd_functions(path):
                writers.append((path.relative_to(ROOT).as_posix(), owner))
    assert writers == [
        ("hfa-worker/src/hfa_worker/consumer.py", "_append_terminal_event_with_evidence"),
    ]


def test_production_sources_do_not_delete_results_stream_history():
    offenders = []
    forbidden = (
        "delete(RedisKey.stream_results()",
        "unlink(RedisKey.stream_results()",
        'redis.call("DEL", RedisKey.stream_results()',
        "redis.call('DEL', RedisKey.stream_results()",
    )
    for root_name in ("hfa-control", "hfa-core", "hfa-worker"):
        for path in (ROOT / root_name / "src").rglob("*"):
            if path.suffix not in {".py", ".lua"}:
                continue
            source = path.read_text(encoding="utf-8")
            if any(token in source for token in forbidden):
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []

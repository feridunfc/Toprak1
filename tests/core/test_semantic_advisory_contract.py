import ast
from pathlib import Path

from scripts.cognitive_governance_audit import audit_repo


SEMANTIC_BRIDGE = Path("hfa-agents/src/hfa_agents/integration/semantic_bridge.py")
FEEDBACK_WRITER = Path("hfa-worker/src/hfa_worker/feedback_writer.py")


def _module_assignments(path: Path) -> dict[str, object]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    values: dict[str, object] = {}

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        try:
            values[node.targets[0].id] = ast.literal_eval(node.value)
        except Exception:
            continue

    return values


def test_semantic_bridge_is_marked_advisory_only():
    values = _module_assignments(SEMANTIC_BRIDGE)

    assert values["ADVISORY_ONLY_SURFACE"] is True
    assert values["CANONICAL_AUTHORITY_WRITES_ALLOWED"] is False


def test_feedback_writer_is_marked_non_authoritative():
    values = _module_assignments(FEEDBACK_WRITER)

    assert values["ADVISORY_ONLY_SURFACE"] is True
    assert values["CANONICAL_AUTHORITY_WRITES_ALLOWED"] is False


def test_semantic_advisory_surfaces_have_no_canonical_authority_writes():
    artifact = audit_repo(Path("."))

    assert artifact.status == "PASS"
    assert artifact.findings_count == 0

# Sprint 22B — Cognitive Governance Hardening Contract

Sprint 22B hardens the cognitive, semantic, and feedback surfaces so they cannot become hidden runtime truth authorities.

## Goal

FeedbackWriter, SemanticBridge, cognitive execution, and semantic memory components may produce advisory signals, observations, feedback, and memory records.

They must not directly mutate canonical runtime truth.

## Governed surfaces

Primary surfaces:

- `hfa-worker/src/hfa_worker/feedback_writer.py`
- `hfa-worker/src/hfa_worker/integration/feedback_client.py`
- `hfa-worker/src/hfa_worker/cognitive_executor.py`
- `hfa-worker/src/hfa_worker/scheduler_semantic_hook.py`
- `hfa-agents/src/hfa_agents/integration/semantic_bridge.py`
- `hfa-control/src/hfa_control/feedback/handle_result.py`
- `hfa-semantic/src/hfa_semantic/memory/outcome_writer.py`
- `hfa-semantic/src/hfa_semantic/memory/feedback_loop.py`
- `hfa-semantic/src/hfa_semantic/validation/*`
- `hfa-semantic/src/hfa_semantic/policy/*`

## Allowed writes

Allowed write classes:

- feedback records
- semantic memory records
- advisory scores
- policy verdicts
- validation outcomes
- observability/audit artifacts
- bounded governance-local state

## Forbidden writes

Cognitive and semantic components must not directly write:

- canonical run/task terminal state
- worker ownership or claim/fence state
- scheduler dispatch authority state
- recovery requeue state
- replay truth state
- Redis keys that are owned by Lua/runtime authority paths

## Required behavior

- FeedbackWriter must be classified as feedback-only.
- SemanticBridge must be classified as advisory/bridge-only.
- Semantic memory writes must remain non-authoritative.
- Any Redis/write-like operation in cognitive surfaces must be either:
  - explicitly allowlisted as feedback/memory/governance-local, or
  - flagged by an audit.
- Authority transitions must remain in runtime/Lua/control-plane canonical paths.

## Non-goals

- No cognitive feature rewrite.
- No semantic ranking algorithm rewrite.
- No production policy change.
- No new automatic decision authority.

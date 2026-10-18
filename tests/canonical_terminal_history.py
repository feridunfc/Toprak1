"""Fresh terminal-history preparation for explicitly owned acceptance domains."""
from hfa.config.keys import RedisKey
from hfa_control.run_terminal_event_evidence import (
    ensure_terminal_event_index, backfill_terminal_event_evidence, validate_readiness,
)


async def prepare_terminal_history(redis):
    """Prepare only empty history, or validate the already prepared history."""
    if await redis.exists(RedisKey.stream_results()):
        validate_readiness(await redis.hgetall(RedisKey.run_terminal_event_index()))
        return
    await ensure_terminal_event_index(redis)
    await redis.xgroup_create(RedisKey.stream_results(), "acceptance-bootstrap", id="0", mkstream=True)
    result = await backfill_terminal_event_evidence(redis, completed_at_ms=50)
    assert result.status == "ready" and result.scanned_entries == 0


async def seed_canonical_root(store, *, run_id, tenant_id, task_id, state):
    """Commit actual TASK precursors; never install a manufactured witness.

    The caller must already own and activate its disposable domain and commit
    RUN_CREATE with this exact root. Every step uses the production evaluator
    and Lua store, including F07's owner-aware admission.
    """
    from hfa.authority import (
        AggregateType, AuthorityCommand, AuthorityDecisionCode, AuthorityEntryContext,
        CanonicalAggregateIdentity, OperationType, RedisAuthorityCommitStatus,
        evaluate_authority_commit,
    )
    assert state in {"ready", "scheduled", "running", "done", "failed"}
    identity = CanonicalAggregateIdentity(
        aggregate_type=AggregateType.TASK, run_id=run_id, task_id=task_id,
    )
    chain = [
        (OperationType.TASK_ADMIT, None, "ready", ("READY_QUEUE_IF_READY",)),
        (OperationType.TASK_DISPATCH, "ready", "scheduled", ("CONTROL_NOTIFICATION", "TASK_REQUEST_MESSAGE")),
        (OperationType.TASK_CLAIM, "scheduled", "running", ("RUNNING_SET",)),
        (OperationType.TASK_COMPLETE if state == "done" else OperationType.TASK_FAIL,
         "running", state, ("OUTPUT_PROJECTION", "DEPENDENCY_FANOUT_INTENT") if state == "done"
         else ("DEPENDENCY_FAILURE_FANOUT_INTENT",)),
    ]
    for revision, (operation, before, after, intents) in enumerate(chain):
        metadata = {"run_id": run_id, "task_id": task_id, "tenant_id": tenant_id}
        command = AuthorityCommand(
            aggregate_identity=identity, operation_type=operation,
            operation_id=f"terminal-acceptance:{identity.sha256}:{operation.value}",
            expected_revision=revision, intended_previous_state=before, intended_next_state=after,
            authoritative_payload=metadata, authoritative_metadata_changes=metadata,
            requested_child_effects={}, requested_projection_intents=tuple({"kind": k} for k in intents),
        )
        evaluation = evaluate_authority_commit(
            context=AuthorityEntryContext(authenticated_writer_id="test/canonical-terminal-setup",
                allowed_operations=frozenset({operation}), target_aggregate_identity_sha256=identity.sha256,
                fence_required=True, fence_valid=True),
            command=command, current_revision=revision, current_state=before, receipt_probe=None,
            committed_at_ms=200 + revision, correlation_id=None,
        )
        assert evaluation.decision.code is AuthorityDecisionCode.ACCEPTED
        result = await store.commit(evaluation.commit_plan,
            require_task_owner_binding=operation is OperationType.TASK_ADMIT)
        assert result.status is RedisAuthorityCommitStatus.COMMITTED, result
        if after == state:
            return identity
    raise AssertionError("canonical root setup did not reach requested state")


async def settlement_witnesses(store, run_id):
    """Read exact current RUN and predecessor witnesses from accepted evidence."""
    from hfa_control.run_create_authority import run_create_operation_id
    from hfa_control.run_terminate_authority import run_terminate_identity
    identity = run_terminate_identity(run_id)
    head = await store.get_aggregate_snapshot(identity)
    current = await store.load_receipt_probe(identity, head.operation_id)
    creation = await store.load_receipt_probe(identity, run_create_operation_id(run_id))
    keys, witness = store.authority_head_witness(current.canonical_store_record, current.receipt)
    _, predecessor = store.authority_head_witness(creation.canonical_store_record, creation.receipt)
    return dict(canonical_head_keys=tuple(keys), canonical_head_witness=tuple(witness),
                run_create_witness=tuple(predecessor))

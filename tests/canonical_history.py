"""Install already-durable pre-F02B history for ordering/recovery fixtures only.

F02B intentionally forbids creating RUN termination from the old unbound/runtime
preconditions. F03 still has to defend against that historical state. This is
not a product commit path and must not seed the F02B first-decision oracles.
"""
import hashlib

from hfa.authority.redis_persistence import (
    _canonical_text, _record_payload, _receipt_payload, _transition_index_payload,
)


async def install_historical_run_termination(redis, store, plan):
    record, receipt = plan.record, plan.receipt
    assert record.operation_type == 'RUN_TERMINATE'
    head = await store.get_aggregate_snapshot(record.aggregate_identity)
    assert head.revision == record.from_revision and head.state == record.previous_state
    keys = store.keyspace(record.aggregate_identity_sha256)
    digest = keys.operation_field(record.operation_id)
    record_json = _canonical_text(_record_payload(record))
    receipt_json = _canonical_text(_receipt_payload(receipt))
    index_json = _canonical_text(_transition_index_payload(record))
    intents = store.canonical_projection_intents_json(record.durable_projection_intents)
    def envelope(payload):
        return _canonical_text({'payload':payload, 'storage_sha1':hashlib.sha1(payload.encode()).hexdigest()})
    await redis.hset(keys.operation_records, digest, envelope(record_json))
    await redis.hset(keys.receipts, digest, envelope(receipt_json))
    await redis.hset(keys.transition_indexes, record.transition_id, envelope(index_json))
    shared = dict(transition_id=record.transition_id, canonical_record_hash=record.canonical_record_hash,
                  operation_id=record.operation_id, operation_digest=digest)
    await redis.hset(keys.aggregate, mapping={
        **shared, 'canonical_aggregate_identity_sha256':record.aggregate_identity_sha256,
        'revision':record.to_revision, 'state':record.next_state, 'state_is_null':'0',
        'canonical_command_hash':record.canonical_command_hash,
        'projection_intents_json':intents, 'updated_at_ms':record.committed_at_ms,
    })
    await redis.xadd(keys.transition_log, {**shared, 'aggregate_revision':record.to_revision,
        'canonical_command_hash':record.canonical_command_hash, 'operation_type':record.operation_type,
        'committed_at_ms':record.committed_at_ms, 'record_json':record_json, 'receipt_json':receipt_json})
    await redis.xadd(keys.outbox, {**shared, 'aggregate_revision':record.to_revision,
        'projection_intents_json':intents, 'committed_at_ms':record.committed_at_ms})

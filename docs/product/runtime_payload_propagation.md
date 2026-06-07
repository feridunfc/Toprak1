# Runtime Payload Propagation

## Sprint

Sprint 45 — Runtime Payload Propagation

## Purpose

Sprint 45 ensures that user-submitted CLI/HTTP messages are propagated into the canonical runtime payload consumed by the worker executor.

The product layer must no longer rely on post-processing or result normalization to surface the submitted message.

Sprint 44 currently proves a working local/dev HTTP product API, but the runtime result can still show:

    FAKE_RESPONSE: no_prompt | submitted_message: hello from HTTP API

Sprint 45 closes that gap.

The target behavior is:

    POST /tasks {"message":"hello payload"}
    -> RunRequested payload {"prompt":"hello payload"}
    -> Worker/FakeExecutor receives {"prompt":"hello payload"}
    -> StateStore result input contains {"prompt":"hello payload"}
    -> output_text is "FAKE_RESPONSE: hello payload..."

## Target claim

`USER_MESSAGE_PROPAGATES_TO_RUNTIME_WORKER_PAYLOAD`

## Required flow

1. CLI or HTTP user submits message.
2. Message is encoded into runtime task payload.
3. SchedulerLua enqueue/dispatch preserves payload or payload reference.
4. Worker stream consume loop receives RunRequested.
5. Worker/FakeExecutor receives payload.
6. StateStore result contains the submitted prompt.
7. Product-visible result equals the runtime result.

## PASS requirements

Sprint 45 PASS requires:

- submitted message is present in runtime payload
- RunRequested payload contains submitted message
- executor invocation payload contains submitted message
- StateStore result input contains submitted message
- output_text includes submitted message
- no `no_prompt` fallback appears in product-visible result
- no `submitted_message` patch is required for PASS
- product result normalization is not used for PASS
- production Lua/EVALSHA path remains true
- SchedulerLua Python fallback remains false
- worker stream consume loop remains true
- direct `_process_message` bypass remains false
- FakeExecutor remains the executor
- production LLM is not called
- deployment/release/operator actions are not attempted

## Forbidden as PASS

Sprint 45 must not report PASS if any of these are used:

- product-only result rewriting
- `no_prompt | submitted_message: ...`
- direct `_process_message` injection
- SchedulerLua Python fallback
- production LLM call
- deployment
- release tag
- operator action buttons
- production-ready claim

## Required artifact

Artifact:

- `docs/dashboard/artifacts/latest_runtime_payload_propagation.json`

Minimum PASS shape:

    {
      "source": "runtime_payload_propagation",
      "status": "PASS",
      "target_claim_supported": true,
      "submitted_message": "hello payload",
      "runtime_payload_propagated": true,
      "run_requested_payload": {
        "prompt": "hello payload"
      },
      "executor_payload": {
        "prompt": "hello payload"
      },
      "state_store_result_input": {
        "prompt": "hello payload"
      },
      "output_text": "FAKE_RESPONSE: hello payload",
      "product_result_normalization_used": false,
      "no_prompt_fallback_used": false,
      "submitted_message_patch_used": false,
      "production_lua_evalsha_path_used": true,
      "scheduler_lua_python_fallback_used": false,
      "worker_stream_consume_loop_used": true,
      "direct_process_message_call_used": false,
      "fake_executor_used": true,
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "operator_action_buttons": false,
      "failing_reasons": []
    }

## Implementation guidance

Preferred fix:

- propagate `payload` as a canonical runtime field into the RunRequested stream message
- deserialize payload in WorkerConsumer
- pass payload to FakeExecutor
- write result input from the actual executor payload

Alternative only if stream payload is unsafe:

- persist payload during enqueue
- WorkerConsumer loads payload by run_id before executor invocation

In either case, PASS must still use:

- production Lua/EVALSHA dispatch
- Redis stream
- WorkerConsumer stream consume loop
- FakeExecutor
- StateStore-compatible result path

## Test file

Sprint 45 should add:

- `tests/integration/test_runtime_payload_propagation.py`

Minimum assertions:

- runtime payload contains submitted prompt
- executor invocation payload contains submitted prompt
- StateStore result input contains submitted prompt
- output_text contains submitted prompt
- output_text does not contain `no_prompt`
- output_text does not contain `submitted_message`
- product normalization is false
- Lua/EVALSHA path remains true
- worker stream consume loop remains true
- direct process-message bypass remains false

## Known limitations

- FakeExecutor only.
- No production LLM.
- Local/dev HTTP API only.
- No auth/rate limiting yet.
- No production deployment claim.
- No multi-tenant load/fairness proof.

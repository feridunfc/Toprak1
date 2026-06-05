# End-to-End Task Execution Product Path

## Purpose

This contract defines the first product-facing end-to-end task execution path.

The goal is to prove that a tenant-scoped task can move through the real runtime lifecycle:

1. submit
2. enqueue
3. claim
4. execute
5. complete
6. read result

This sprint shifts focus from evidence/dashboard infrastructure to the product core: a user or operator can submit a task and receive a completed result.

## Product claim

After this path is implemented, the system may claim:

`TENANT_SCOPED_TASK_EXECUTION_PATH_VISIBLE`

This is not a production deployment claim.

## Scope

Sprint 37 introduces a safe, deterministic task execution path using a non-production executor.

The initial executor must be one of:

- `EchoExecutor`
- `FakeExecutor`
- `DryRunExecutor`

The executor must not call production LLMs or external paid/side-effecting services.

## Allowed mutation

Unlike previous read-only dashboard/evidence sprints, Sprint 37 allows controlled runtime mutation only for the canonical task lifecycle.

Allowed mutations:

- create tenant-scoped task envelope
- enqueue task through canonical queue/state path
- claim task through canonical worker path
- write execution status
- write completion result
- write read-model/demo artifact

Allowed mutation must remain limited to canonical task lifecycle operations.

## Forbidden mutation

The product path must not:

- mutate Redis outside canonical task lifecycle
- bypass StateStore/Lua/runtime authority boundaries
- mutate canonical state through ad-hoc scripts
- write unauthorized recovery state
- requeue tasks outside the explicit task lifecycle test
- auto-resume workers or queues
- create deployment state
- create release tag state

## Forbidden operations

The path must not:

- deploy
- create release tags
- call production LLMs
- call external side-effecting services
- expose operator requeue buttons
- expose auto-resume buttons
- expose production promotion controls
- assert production-ready status

## Required CLI/product commands

Minimum CLI commands:

### Submit task

    python scripts/submit_task.py --tenant demo --type echo --payload '{"message":"hello"}'

Expected output:

    {
      "status": "SUBMITTED",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "..."
    }

### Run worker once

    python scripts/run_worker_once.py --tenant demo

Expected output:

    {
      "status": "COMPLETED",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "...",
      "executor": "echo",
      "result": {
        "echo": "hello"
      }
    }

### Read task result

    python scripts/get_task_result.py --run-id "..."

Expected output:

    {
      "status": "COMPLETED",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "...",
      "result": {
        "echo": "hello"
      }
    }

## Required dashboard/read model

The sprint should produce a minimal read-only dashboard artifact:

- `docs/dashboard/artifacts/latest_task_execution_demo.json`

Minimum shape:

    {
      "source": "task_execution_demo",
      "status": "PASS",
      "tenant_id": "demo",
      "task_id": "...",
      "run_id": "...",
      "task_type": "echo",
      "lifecycle": [
        "SUBMITTED",
        "QUEUED",
        "CLAIMED",
        "EXECUTED",
        "COMPLETED"
      ],
      "result": {
        "echo": "hello"
      },
      "production_llm_call_attempted": false,
      "deployment_attempted": false,
      "release_tag_created": false,
      "noncanonical_redis_mutation_attempted": false
    }

## Required tests

The sprint must include an integration or core E2E test that verifies:

- tenant-scoped task can be submitted
- task is queued through canonical path
- worker claims exactly one task
- safe executor runs
- task completes
- result is readable by run id
- task execution demo artifact is written
- production LLM call is not attempted
- deployment is not attempted
- release tag is not created
- noncanonical Redis mutation is not attempted

## Safety boundaries

The system may execute the safe task lifecycle, but it must not create production authority.

This means:

- task lifecycle mutation is allowed
- deployment mutation is forbidden
- release mutation is forbidden
- recovery/requeue authorization is forbidden unless explicitly part of the canonical task lifecycle
- production LLM call is forbidden

## Non-goals

This sprint does not:

- implement real LLM task execution
- implement multi-tenant fairness demo
- implement production deployment
- implement release tagging
- implement operator requeue UI
- implement auto-resume UI
- implement long-running worker service
- assert production-ready status

## Governance rule

The E2E product path is the first product-core proof.

It must use the real runtime lifecycle where available and must only fall back to a safe in-memory/fake adapter when the repository does not yet expose a stable canonical API.

Any fallback must be documented in the output artifact and tests.

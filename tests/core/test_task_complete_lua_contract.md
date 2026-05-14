# V23.1 task_complete.lua contract

## Inputs
- Current task state must be `running` or `scheduled`
- Terminal target must be `done` or `failed`
- Run state must not already be terminal

## Guarantees
- Duplicate completion is idempotent (`already_terminal`)
- `failed` does not unlock descendants
- `done` decrements each direct child exactly once
- Child readiness emission is deduped by `ready_emitted`
- No recursive descendant walk
- Missing child metadata hard-fails the transaction result

## Required follow-up tests
- child remaining_deps negative clamp
- child already ready does not duplicate queue member
- child terminal does not re-enter queue
- run terminal block prevents late completion mutation

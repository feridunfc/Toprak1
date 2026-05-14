
# V24.2B — Input Resolution / Hydration

## Goal
Hydrate child task inputs from parent task outputs.

## What this pack adds
- InputResolver
- recursive placeholder traversal
- batched parent output fetch via MGET
- fail-fast missing parent output protection
- core + integration tests

## Placeholder format
- ${task_id.output}

## Rules
- missing parent output raises MissingParentOutputError
- exact-placeholder strings inject parsed JSON when possible
- embedded placeholders inject raw string output
- repeated retries resolve to the same hydrated input as long as parent outputs remain unchanged

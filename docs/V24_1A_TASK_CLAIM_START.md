
# V24.1A — Task Claim Start

## Goal
Atomic worker claim boundary:
- scheduled -> running
- owner set in meta
- running zset enrollment
- duplicate execution stopped before Python execution begins

## Included
- `task_claim_start.lua`
- `TaskClaimManager`
- worker consumer skeleton
- claim contract + integration tests

## Validated
- scheduled task can be claimed once
- running task cannot be claimed again
- terminal task rejected
- worker consumer only executes after successful claim

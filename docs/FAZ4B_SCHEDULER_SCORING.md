
# Faz 4B — Unified Scheduler Scoring

## Goal
Combine fairness, load, and capacity into a single deterministic decision function.

## Order
1. vruntime (fairness)
2. inflight (fairness tie-break)
3. worker load
4. capacity (prefer higher)
5. deterministic ids

## Output
Single best candidate for reservation + dispatch.

## Next
Faz 4C: wire scoring into fairness dispatcher + reservation pipeline

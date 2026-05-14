
# Faz 2.1 — Reservation Durability

## Goal
Harden reservation lifecycle beyond basic authority wiring.

## What this pack adds
- scheduler_id tracked inside reservations
- reservation renewal API
- reaper listing/orphan diagnostics with scheduler ownership
- core + integration tests

## Why it matters
This improves reservation forensics, renewal safety, and control-plane debuggability before moving into fairness and predictive scheduling.

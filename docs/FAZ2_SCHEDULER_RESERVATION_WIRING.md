
# Faz 2 — Scheduler Reservation Wiring

## Goal
Make reservation active in the scheduler path, not just available in worker claim path.

## What this pack adds
- SchedulerReservationDispatcher
- reserve -> dispatch -> claim lifecycle bridge
- release on dispatch failure
- keep reservation on dispatch success so worker claim can consume it

## Invariants
- reservation conflict blocks dispatch
- successful dispatch does not prematurely release reservation
- failed dispatch releases reservation
- worker claim consumes reservation on success

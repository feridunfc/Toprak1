
# FailureSweeper Integration Pack

Validates lazy failure propagation on real Redis:

- pending -> blocked_by_failure
- ready -> blocked_by_failure
- terminal states untouched
- idempotency preserved

Run:
    pytest tests/integration/test_failure_sweeper_integration.py -vv

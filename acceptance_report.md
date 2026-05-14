## Extended core test note

Command:
`python -m pytest tests/core -q --tb=short`

Result:
`122 passed, 10 failed, 1 warning`

Assessment:
The failures are outside Sprint 1 allowed implementation scope and pre-existing repository drift areas:
- DAG dispatch input contract
- worker input resolver behavior
- worker_main_patch.py syntax
- payload store return compatibility
- task heartbeat mock compatibility
- fake executor legacy result compatibility

Sprint 1 acceptance smoke tests passed with and without `IRON_V3_EVENT_GATE`.
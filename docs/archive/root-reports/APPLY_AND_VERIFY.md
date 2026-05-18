
# Apply & verify

Overwrite:
- hfa-core/src/hfa/runtime/state_store.py
- hfa-worker/src/hfa_worker/input_resolver.py

Run:
```powershell
.\.venv\Scripts\python -m pytest tests/core/test_payload_store_hardening_core.py -vv
```

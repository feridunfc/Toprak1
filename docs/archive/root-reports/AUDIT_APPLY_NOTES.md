
# 8B integration apply notes

## Files
- tests/integration/test_lineage_flow_integration.py

## Verify
```powershell
.\.venv\Scripts\python -m pytest tests/integration/test_lineage_flow_integration.py -vv
```

## What this proves
- producer lineage persistence
- consumer lineage recording
- run-level edge graph creation
- duplicate consume suppression
- checksum guard before lineage write
- run_id isolation for same task_id

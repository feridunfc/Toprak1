
# V24.2A — Output Persistence

## Goal
Persist successful parent task outputs separately from task meta.

## What this pack adds
- task_complete.lua output persistence for successful completions
- DagRedisKey.task_output(task_id)
- DagLua.task_complete(..., output_data=...)
- contract + integration tests

## Rules
- output is stored only for done
- failed tasks do not write output
- output key gets DAG-aligned TTL
- output is stored outside task meta

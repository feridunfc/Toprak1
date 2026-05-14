
# V24.2C — Merge & Lineage

## Goal
Move from simple placeholder hydration to structured multi-parent dataflow and provenance tracking.

## What this pack adds
- `__merge__` directive support
- list and dict aggregation modes
- mixed JSON/string merge safety
- lineage record creation
- lineage persistence key

## Merge formats

### List merge
{"merged": {"__merge__": ["task_1", "task_2"], "mode": "list"}}

### Dict merge
{"inputs": {"__merge__": {"left": "task_a", "right": "task_b"}, "mode": "dict"}}

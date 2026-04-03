"""
hfa-semantic/QUICKSTART.md

IRONCLAD Semantic Layer — Quick Start Guide

## 5-Minute Setup

### Development (In-Memory)

```bash
cd hfa-semantic

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run in development mode
export SEMANTIC_STATE_BACKEND=memory
python -m pytest tests/test_runtime_foundation.py -v
```

### Production (Redis-Backed)

```bash
# Configure Redis
export SEMANTIC_STATE_BACKEND=redis
export SEMANTIC_REDIS_URL=redis://localhost:6379

# Install
pip install -e .

# Run service (future)
python -m hfa_semantic.service

# View metrics
curl http://localhost:8000/metrics
```

## Configuration Checklist

- [ ] SEMANTIC_ENABLED=true (or false to disable)
- [ ] SEMANTIC_STATE_BACKEND=redis|memory
- [ ] SEMANTIC_REDIS_URL=redis://... (if redis backend)
- [ ] SEMANTIC_MAX_PARTITIONS_PER_RULE=10000
- [ ] SEMANTIC_STATE_TTL_MS=3600000

## Usage Example

### Create State Store

```python
from hfa_semantic.runtime import RedisStateStore, InMemoryStateStore

# Development
store = InMemoryStateStore()

# Production
import redis.asyncio as redis
redis_client = redis.from_url("redis://localhost:6379")
store = RedisStateStore(redis_client)
```

### Check for Duplicates

```python
from hfa_semantic.runtime import DedupStore

dedup = DedupStore(redis_client)

# Check if duplicate
is_dup = await dedup.is_duplicate("event_12345")

# Mark as processed
await dedup.mark_processed("event_12345", event_time_ms)
```

### Track Event Ordering

```python
from hfa_semantic.runtime import Watermark, LatenessPolicy

watermark = Watermark(allowed_lateness_ms=10_000)

# Accept if on-time
is_late = watermark.should_accept(event_time_ms, LatenessPolicy.DROP)
```

### Store Semantic State

```python
# Store reasoning state
await store.put(
    rule_id="pattern_a",
    partition="entity:123",
    state={"count": 5, "events": [...]},
    ttl_ms=3_600_000  # 1 hour
)

# Retrieve
state = await store.get("pattern_a", "entity:123")

# Evict if too many partitions
evicted = await store.evict("pattern_a", max_partitions=1000)
```

## Testing

### Unit Tests

```bash
# All tests
pytest tests/ -v

# Specific test
pytest tests/test_runtime_foundation.py::test_inmemory_state_store_put_get -v

# Only async tests
pytest tests/ -v -m asyncio

# Only Redis tests
pytest tests/ -v -k redis
```

### Debugging

```bash
# Verbose output
pytest tests/ -vv

# Show print statements
pytest tests/ -s

# Stop on first failure
pytest tests/ -x

# Specific test file
pytest tests/test_runtime_foundation.py -v
```

## Common Issues

### Q: ImportError when running tests

**A**: Install package in editable mode:
```bash
pip install -e ".[dev]"
```

### Q: Redis connection refused

**A**: Start Redis:
```bash
redis-server
# or
docker run -p 6379:6379 redis
```

### Q: Tests timeout

**A**: Increase pytest timeout:
```bash
pytest tests/ -v --timeout=30
```

## Next Steps

1. Read `ARCHITECTURE.md` for component details
2. Read `INTEGRATION_GUIDE.md` to integrate with scheduler
3. Run tests and explore examples
4. Configure for your environment

## Support

See `README.md` for full documentation.
"""


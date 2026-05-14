import pytest
import time
from fakeredis import FakeAsyncRedis
from hfa_semantic.api.models import ValidatedOutcome
from hfa_semantic.memory.semantic_memory_v2 import SemanticMemoryV2

@pytest.mark.asyncio
async def test_semantic_memory_survives_pod_restart():
    # 1. FakeRedis ile Dagitik Hafiza Sunucusu Baslat
    r = FakeAsyncRedis(decode_responses=False)
    await r.flushdb()

    # 2. POD 1 (Ajan calisti, karar verdi ve hafizaya yazdi)
    memory_pod_1 = SemanticMemoryV2(redis_client=r, max_items=100)
    
    # PYDANTIC KATI KONTROLU ICIN YASAL ENUM DEGERLERI
    outcome = ValidatedOutcome(
        event_id="evt-kriz-001",
        outcome_type="success",   # <--- SISTEMIN ISTEDIGI DEGER
        validated=True,
        validator="rule",         # <--- SISTEMIN ISTEDIGI DEGER
        confidence=0.99,
        timestamp_ms=int(time.time() * 1000)
    )
    await memory_pod_1.append(outcome)

    # 3. POD 1 COKTU (RAM silindi)
    del memory_pod_1
    
    # 4. YENI POD GELDI (Instance degisti ama Redis ayni)
    memory_pod_2 = SemanticMemoryV2(redis_client=r, max_items=100)
    
    # 5. ISPAT: Veri Redis uzerinde hayatta kalmis mi?
    keys = await r.keys("semantic:memory:outcome:*")
    assert len(keys) == 1, "FATAL: Otonom hafiza silindi! Veriler RAM'de kalmis."
    assert b"evt-kriz-001" in keys[0], "FATAL: Yanlis event kaydedildi."
    
    # 6. ISPAT 2: ZSET (Eviction index) calisiyor mu?
    index_count = await r.zcard("semantic:memory:index")
    assert index_count == 1, "FATAL: Eviction ZSET indeksi olusturulmamis!"

    await r.close()
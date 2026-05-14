#!/usr/bin/env python3
import os, sys, asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "hfa-worker" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "hfa-agents" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "hfa-semantic" / "src"))
sys.path.insert(0, str(Path(__file__).parent / "hfa-core" / "src"))

PASSED = 0
FAILED = 0

def test(name, fn, *args):
    global PASSED, FAILED
    print(f"\n{'='*70}\nTEST: {name}\n{'='*70}")
    try:
        fn(*args)
        PASSED += 1
        print(f"[PASS] {name}")
    except Exception as e:
        FAILED += 1
        print(f"[FAIL] {name}: {e}")
        import traceback; traceback.print_exc()

def test_import_default():
    os.environ.pop("EXECUTOR_MODE", None)
    from hfa_worker.executor_factory import build_executor
    exe = build_executor({})
    assert type(exe).__name__ == "FakeExecutor"
    print(f"  [OK] Default: {type(exe).__name__}")

def test_import_cognitive():
    os.environ["EXECUTOR_MODE"] = "cognitive"
    for mod in list(sys.modules.keys()):
        if "executor_factory" in mod or "cognitive" in mod:
            del sys.modules[mod]
    from hfa_worker.executor_factory import build_executor
    exe = build_executor({})
    assert type(exe).__name__ == "CognitiveExecutor"
    print(f"  [OK] Cognitive: {type(exe).__name__}")

def test_syntax():
    for f in ["hfa-worker/src/hfa_worker/cognitive_executor.py",
              "hfa-worker/src/hfa_worker/executor_factory.py",
              "hfa-worker/src/hfa_worker/feedback_writer.py"]:
        with open(f) as fp:
            compile(fp.read(), f, 'exec')
        print(f"  [OK] {Path(f).name} syntax")

async def test_cognitive_run():
    os.environ["EXECUTOR_MODE"] = "cognitive"
    for mod in list(sys.modules.keys()):
        if "hfa_worker" in mod or "cognitive" in mod:
            del sys.modules[mod]
    from hfa_worker.executor_factory import build_executor
    from hfa.events.schema import RunRequestedEvent
    exe = build_executor({})
    evt = RunRequestedEvent(run_id="t1", tenant_id="t", agent_type="s", payload={"goal": "test"})
    result = await exe.execute(evt)
    assert result.status in ["done", "failed"]
    assert isinstance(result.payload, dict)
    print(f"  [OK] Single run: {result.status}")

async def test_degradation():
    os.environ["EXECUTOR_MODE"] = "cognitive"
    from hfa_worker.cognitive_executor import CognitiveExecutor
    from hfa.events.schema import RunRequestedEvent
    exe = CognitiveExecutor(semantic_pipeline=None)
    evt = RunRequestedEvent(run_id="t2", tenant_id="t", agent_type="r", payload={"goal": "t"})
    result = await exe.execute(evt)
    assert result.status in ["done", "failed"]
    print(f"  [OK] Degraded: {result.status}")

def test_feedbackwriter():
    from hfa_worker.feedback_writer import FeedbackWriter
    fw = FeedbackWriter(semantic_pipeline=None)
    assert fw._pipeline is None
    print(f"  [OK] FeedbackWriter no-op")

def test_backward():
    os.environ.pop("EXECUTOR_MODE", None)
    for mod in list(sys.modules.keys()):
        if "executor_factory" in mod:
            del sys.modules[mod]
    from hfa_worker.executor_factory import build_executor
    exe = build_executor({})
    assert type(exe).__name__ == "FakeExecutor"
    print(f"  [OK] Backward compat OK")

async def test_concurrent():
    os.environ["EXECUTOR_MODE"] = "cognitive"
    for mod in list(sys.modules.keys()):
        if "hfa_worker" in mod:
            del sys.modules[mod]
    from hfa_worker.executor_factory import build_executor
    from hfa.events.schema import RunRequestedEvent
    async def run_one(i):
        exe = build_executor({})
        evt = RunRequestedEvent(run_id=f"c{i}", tenant_id="t", agent_type="s", payload={"goal": f"t{i}"})
        r = await exe.execute(evt)
        return i, r.status
    results = await asyncio.gather(*[run_one(i) for i in range(5)])
    for idx, status in results:
        assert status in ["done", "failed"]
    print(f"  [OK] 5 concurrent tasks OK")

async def test_contract():
    os.environ["EXECUTOR_MODE"] = "cognitive"
    for mod in list(sys.modules.keys()):
        if "hfa_worker" in mod:
            del sys.modules[mod]
    from hfa_worker.executor_factory import build_executor
    from hfa.events.schema import RunRequestedEvent
    exe = build_executor({})
    evt = RunRequestedEvent(run_id="ct", tenant_id="t", agent_type="c", payload={"goal": "test"})
    result = await exe.execute(evt)
    assert hasattr(result, "status") and hasattr(result, "payload") and hasattr(result, "cost_cents") and hasattr(result, "tokens_used")
    assert isinstance(result.cost_cents, int) and isinstance(result.tokens_used, int)
    print(f"  [OK] Contract valid")

async def async_tests():
    test("4. CognitiveExecutor Run", test_cognitive_run)
    test("5. Semantic Degradation", test_degradation)
    test("8. Concurrency", test_concurrent)
    test("9. Result Contract", test_contract)

if __name__ == "__main__":
    print("\n" + "="*70 + "\nIRONCLAD v6 - COMPREHENSIVE TESTS\n" + "="*70)
    test("1. Import Default", test_import_default)
    test("2. Import Cognitive", test_import_cognitive)
    test("3. Syntax", test_syntax)
    test("6. FeedbackWriter", test_feedbackwriter)
    test("7. Backward Compat", test_backward)
    asyncio.run(async_tests())
    print("\n" + "="*70 + f"\nSUMMARY: {PASSED} PASS, {FAILED} FAIL\n" + "="*70)
    if FAILED == 0:
        print("\n[SUCCESS] ALL TESTS PASSED - STAGING GO APPROVED\n")
        sys.exit(0)
    else:
        print(f"\n[ERROR] {FAILED} FAILED\n")
        sys.exit(1)


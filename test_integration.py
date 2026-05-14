"""
IRONCLAD v6 Integration Test
Cognitive Executor + Semantic Runtime Pipeline
"""
import asyncio
from hfa_worker.executor_factory import build_executor


async def test_full_integration():
    print("\n" + "="*60)
    print("IRONCLAD v6 Full Integration Test")
    print("="*60)
    
    # Test 1: Build all executor modes
    print("\n[Test 1] Building all executor modes...")
    modes_config = [
        {"executor_mode": "fake", "desc": "Fake (Mock) Mode"},
        {"executor_mode": "openai", "desc": "OpenAI Mode", "openai_api_key": "sk-test"},
        {"executor_mode": "cognitive", "desc": "Cognitive (Semantic) Mode", "cognitive_budget_cents": 5000},
    ]
    
    for cfg in modes_config:
        try:
            executor = build_executor(cfg)
            print(f"  ✓ {cfg['desc']}: {type(executor).__name__}")
        except Exception as e:
            print(f"  ✗ {cfg['desc']}: {e}")
            return False
    
    # Test 2: Verify CognitiveExecutor has semantic pipeline
    print("\n[Test 2] Verifying CognitiveExecutor semantic pipeline...")
    cognitive_config = {
        "executor_mode": "cognitive",
        "cognitive_budget_cents": 3000,
    }
    executor = build_executor(cognitive_config)
    if executor._semantic:
        print(f"  ✓ Semantic Pipeline initialized: {type(executor._semantic)}")
    else:
        print(f"  ✗ Semantic Pipeline failed to initialize")
        return False
    
    # Test 3: Verify budget setting
    print("\n[Test 3] Verifying executor budget...")
    if executor._max_budget == 3000:
        print(f"  ✓ Budget correctly set to 3000¢")
    else:
        print(f"  ✗ Budget mismatch: {executor._max_budget}¢")
        return False
    
    print("\n" + "="*60)
    print("✓ ALL INTEGRATION TESTS PASSED")
    print("="*60)
    return True


if __name__ == "__main__":
    result = asyncio.run(test_full_integration())
    exit(0 if result else 1)


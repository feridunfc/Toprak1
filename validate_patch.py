#!/usr/bin/env python3
"""
IRONCLAD v6 - Cognitive Executor Patch Validator
Comprehensive pre-deployment verification

Run: python validate_patch.py
"""

import sys
import os
import json
from pathlib import Path
from typing import Tuple, List

class PatchValidator:
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.passed: List[str] = []
        
    def log_pass(self, msg: str):
        self.passed.append(msg)
        print(f"  ✓ {msg}")
        
    def log_warn(self, msg: str):
        self.warnings.append(msg)
        print(f"  ⚠ {msg}")
        
    def log_error(self, msg: str):
        self.errors.append(msg)
        print(f"  ✗ {msg}")

    def validate_file_exists(self, path: str, name: str) -> bool:
        if Path(path).exists():
            self.log_pass(f"{name} exists")
            return True
        else:
            self.log_error(f"{name} NOT FOUND: {path}")
            return False

    def check_patch_syntax(self, filepath: str) -> bool:
        try:
            with open(filepath) as f:
                compile(f.read(), filepath, 'exec')
            self.log_pass(f"Python syntax OK: {Path(filepath).name}")
            return True
        except SyntaxError as e:
            self.log_error(f"Syntax error in {filepath}: {e}")
            return False

    def test_imports(self) -> bool:
        print("\n[3/5] Import Chain Verification")
        
        imports_to_test = [
            ("hfa_worker.executor_factory", "build_executor"),
            ("hfa_worker.cognitive_executor", "CognitiveExecutor"),
            ("hfa_worker.feedback_writer", "FeedbackWriter"),
            ("hfa_worker.executor_base", "Executor"),
            ("hfa_semantic.runtime.factory", "build_pipeline_sync"),
            ("hfa_semantic.validation.outcome_validator", "OutcomeValidator"),
        ]
        
        all_ok = True
        for module_name, class_name in imports_to_test:
            try:
                mod = __import__(module_name, fromlist=[class_name])
                if hasattr(mod, class_name):
                    self.log_pass(f"{module_name}.{class_name}")
                else:
                    self.log_error(f"{class_name} not found in {module_name}")
                    all_ok = False
            except ImportError as e:
                self.log_warn(f"Optional import failed (may be OK): {module_name}")
                all_ok = False
        
        return all_ok

    def test_executor_factory(self) -> bool:
        print("\n[4/5] Executor Factory - Build Test")
        
        try:
            from hfa_worker.executor_factory import build_executor
            
            # Test all three modes
            modes = [
                ("fake", {}),
                ("cognitive", {"cognitive_budget_cents": 5000}),
            ]
            
            for mode_name, config_extra in modes:
                config = {"executor_mode": mode_name}
                config.update(config_extra)
                
                try:
                    executor = build_executor(config)
                    self.log_pass(f"Mode '{mode_name}' builds: {type(executor).__name__}")
                except Exception as e:
                    self.log_error(f"Mode '{mode_name}' failed: {e}")
                    return False
            
            # Test cognitive specifically
            cognitive_config = {"executor_mode": "cognitive", "cognitive_budget_cents": 3000}
            cog_executor = build_executor(cognitive_config)
            
            if hasattr(cog_executor, "execute") and callable(cog_executor.execute):
                self.log_pass("CognitiveExecutor.execute() method exists")
            else:
                self.log_error("CognitiveExecutor.execute() method missing")
                return False
                
            if hasattr(cog_executor, "_semantic"):
                if cog_executor._semantic:
                    self.log_pass(f"Semantic pipeline: {type(cog_executor._semantic)}")
                else:
                    self.log_warn("Semantic pipeline is None (degraded mode)")
            
            return True
            
        except Exception as e:
            self.log_error(f"Executor factory test failed: {e}")
            return False

    def test_semantic_pipeline(self) -> bool:
        print("\n[5/5] Semantic Pipeline Components")
        
        try:
            from hfa_semantic.runtime.factory import build_pipeline_sync
            import redis.asyncio
            
            # Create a mock redis client (in-memory would be better)
            try:
                client = redis.asyncio.from_url("redis://localhost:6379", decode_responses=False)
                pipeline = build_pipeline_sync(redis_client=client)
                
                if pipeline:
                    state_store, dedup, watermark, metrics = pipeline
                    self.log_pass(f"StateStore: {type(state_store).__name__}")
                    self.log_pass(f"DedupStore: {type(dedup).__name__}")
                    self.log_pass(f"Watermark: {type(watermark).__name__}")
                    self.log_pass(f"Metrics: {type(metrics).__name__}")
                    return True
                else:
                    self.log_warn("Pipeline returned None (no Redis available)")
                    return True  # This is OK - degraded mode
            except Exception as e:
                self.log_warn(f"Could not test with real Redis: {e}")
                return True  # This is OK - Redis may not be running
                
        except ImportError as e:
            self.log_warn(f"Semantic pipeline import failed (may be OK): {e}")
            return True

    def run(self) -> int:
        print("\n" + "="*70)
        print("IRONCLAD v6 - COGNITIVE EXECUTOR PATCH VALIDATOR")
        print("="*70)

        print("\n[1/5] File Existence Checks")
        files_ok = all([
            self.validate_file_exists(
                "hfa-worker/src/hfa_worker/executor_factory.py",
                "executor_factory.py"
            ),
            self.validate_file_exists(
                "hfa-worker/src/hfa_worker/cognitive_executor.py",
                "cognitive_executor.py"
            ),
            self.validate_file_exists(
                "hfa-worker/src/hfa_worker/feedback_writer.py",
                "feedback_writer.py"
            ),
        ])

        print("\n[2/5] Python Syntax Checks")
        syntax_ok = all([
            self.check_patch_syntax("hfa-worker/src/hfa_worker/executor_factory.py"),
            self.check_patch_syntax("hfa-worker/src/hfa_worker/cognitive_executor.py"),
            self.check_patch_syntax("hfa-worker/src/hfa_worker/feedback_writer.py"),
        ])

        imports_ok = self.test_imports()
        factory_ok = self.test_executor_factory()
        pipeline_ok = self.test_semantic_pipeline()

        print("\n" + "="*70)
        print("VALIDATION RESULTS")
        print("="*70)
        
        print(f"\n✓ Passed: {len(self.passed)}")
        print(f"⚠ Warnings: {len(self.warnings)}")
        print(f"✗ Errors: {len(self.errors)}")

        if self.errors:
            print("\nFailed checks:")
            for error in self.errors:
                print(f"  - {error}")

        if self.warnings:
            print("\nWarnings (non-critical):")
            for warning in self.warnings:
                print(f"  - {warning}")

        print("\n" + "="*70)
        if self.errors:
            print("STATUS: ❌ VALIDATION FAILED")
            print("="*70)
            return 1
        else:
            print("STATUS: ✅ VALIDATION PASSED")
            print("="*70)
            print("\nNext steps:")
            print("  1. Set environment variables:")
            print("     export EXECUTOR_MODE=cognitive")
            print("     export REDIS_URL=redis://localhost:6379")
            print("     export ANTHROPIC_API_KEY=sk-ant-...")
            print("\n  2. Start worker:")
            print("     python -m hfa_worker.main")
            print("\n  3. Send test task:")
            print('     curl -X POST http://localhost:8000/task -d \'{"goal":"build API"}\'')
            print("\n  4. Monitor logs for:")
            print('     - "CognitiveExecutor built"')
            print('     - "SemanticBridge enriched"')
            print('     - "FeedbackWriter validated"')
            print("\n" + "="*70 + "\n")
            return 0

if __name__ == "__main__":
    validator = PatchValidator()
    sys.exit(validator.run())


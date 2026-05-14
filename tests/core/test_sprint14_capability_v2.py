from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "hfa-control" / "src"))

from hfa_control.registry import WorkerRegistry


class TestSprint14CapabilityV2:
    def test_legacy_list_matches(self):
        registry = WorkerRegistry(None, None)

        worker_cap = ["gpu", "python", "node"]

        assert registry._capability_matches("gpu", worker_cap) is True
        assert registry._capability_matches("java", worker_cap) is False

    def test_structured_dict_exact_match(self):
        registry = WorkerRegistry(None, None)

        worker_cap = {
            "gpu": "a100",
            "python": "3.10",
            "region": "us-east",
        }

        assert registry._capability_matches({"gpu": "a100"}, worker_cap) is True
        assert registry._capability_matches({"gpu": "v100"}, worker_cap) is False

    def test_structured_dict_key_presence(self):
        registry = WorkerRegistry(None, None)

        worker_cap = {
            "gpu": "a100",
            "python": "3.10",
        }

        assert registry._capability_matches({"gpu": ""}, worker_cap) is True
        assert registry._capability_matches({"cuda": ""}, worker_cap) is False

    def test_string_requirement_with_structured_worker(self):
        registry = WorkerRegistry(None, None)

        worker_cap = {"gpu": "a100"}

        assert registry._capability_matches("gpu", worker_cap) is True
        assert registry._capability_matches("cuda", worker_cap) is False

    def test_mixed_compatibility_legacy_worker_structured_requirement(self):
        registry = WorkerRegistry(None, None)

        worker_cap = ["gpu", "python"]
        requirement = {"gpu": "a100"}

        assert registry._capability_matches(requirement, worker_cap) is False

    def test_version_exact_match_only_in_14a(self):
        registry = WorkerRegistry(None, None)

        worker_cap = {"python": "3.10"}

        assert registry._capability_matches({"python": "3.10"}, worker_cap) is True
        assert registry._capability_matches({"python": "3.9"}, worker_cap) is False
        assert registry._capability_matches({"python": ">=3.9"}, worker_cap) is False

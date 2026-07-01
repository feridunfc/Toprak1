from pathlib import Path


def test_scheduler_loop_legacy_injected_fallback_is_explicitly_gated():
    source = Path("hfa-control/src/hfa_control/scheduler_loop.py").read_text(encoding="utf-8")

    assert "HFA_ALLOW_LEGACY_INJECTED_DISPATCH" in source
    assert "def _env_allows_legacy_injected_dispatch()" in source
    assert "if not _env_allows_legacy_injected_dispatch():" in source
    assert "return await self._legacy_injected_dispatch_once(snapshot)" in source


def test_scheduler_loop_legacy_injected_fallback_is_not_unconditional():
    source = Path("hfa-control/src/hfa_control/scheduler_loop.py").read_text(encoding="utf-8")
    unconditional = "        return await self._legacy_injected_dispatch_once(snapshot)"

    assert source.count(unconditional) == 1
    assert "if not _env_allows_legacy_injected_dispatch():" in source
    assert source.index("if not _env_allows_legacy_injected_dispatch():") < source.index(unconditional)


def test_scheduler_loop_marks_legacy_injected_path_non_production_authority():
    source = Path("hfa-control/src/hfa_control/scheduler_loop.py").read_text(encoding="utf-8")

    assert "not a production dispatch authority" in source
    assert "legacy injected fallback" in source

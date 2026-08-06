from __future__ import annotations

import importlib
import importlib.util
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


LUA_MEMBER = "hfa/lua/admission_resource_reservation.lua"


def test_new_module_imports_without_restoring_legacy_quota_manager():
    assert importlib.util.find_spec(
        "hfa.governance.admission_resource_reservation"
    ) is not None
    assert importlib.util.find_spec("hfa.governance.quota_manager") is None

    module = importlib.import_module(
        "hfa.governance.admission_resource_reservation"
    )
    assert hasattr(module, "AdmissionResourceReservationManager")
    assert not hasattr(module, "QuotaManager")


def test_exact_parent_admission_optional_import_fallback_is_unchanged():
    sys.modules.pop("hfa_control.admission", None)
    admission = importlib.import_module("hfa_control.admission")
    assert admission.QuotaManager is None
    assert admission.validate_run_id_format is None


def _build_wheel(tmp_path: Path) -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    hfa_core = repo_root / "hfa-core"
    source_copy = tmp_path / "hfa-core-source"
    shutil.copytree(
        hfa_core,
        source_copy,
        ignore=shutil.ignore_patterns(
            "build", "dist", "*.egg-info", "__pycache__", "*.pyc"
        ),
    )
    wheel_dir = tmp_path / "wheelhouse"
    wheel_dir.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--no-build-isolation",
            "--wheel-dir",
            str(wheel_dir),
            str(source_copy),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    wheels = list(wheel_dir.glob("hfa_core-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_built_wheel_contains_admission_reservation_lua(tmp_path):
    wheel = _build_wheel(tmp_path)
    with zipfile.ZipFile(wheel) as archive:
        assert LUA_MEMBER in archive.namelist()


def test_wheel_installed_manager_resolves_and_loads_lua(tmp_path):
    wheel = _build_wheel(tmp_path)
    target = tmp_path / "installed"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(target),
            str(wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    code = r'''
import asyncio
from pathlib import Path
from hfa.governance.admission_resource_reservation import (
    AdmissionResourceReservationManager,
)

class Redis:
    async def script_load(self, source):
        assert "Operation-scoped admission resource reservation" in source
        return "a" * 40

async def main():
    manager = AdmissionResourceReservationManager(Redis())
    await manager.initialise()
    path = Path(manager._loader._path)
    assert path.is_file()
    assert path.name == "admission_resource_reservation.lua"

asyncio.run(main())
'''
    env = os.environ.copy()
    env["PYTHONPATH"] = str(target)
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

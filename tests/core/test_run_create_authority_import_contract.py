from __future__ import annotations

import importlib
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]



def isolated_hfa_core_copy(root: Path, tmp_path: Path) -> Path:
    source_copy = tmp_path / "hfa-core-source"
    shutil.copytree(
        root / "hfa-core",
        source_copy,
        ignore=shutil.ignore_patterns(
            "build", "dist", "*.egg-info", "__pycache__", "*.pyc"
        ),
    )
    return source_copy

def test_module_import_does_not_restore_legacy_quota_manager():
    module = importlib.import_module("hfa_control.run_create_authority")
    assert module.FEATURE_FLAG == "HFA_CANONICAL_RUN_CREATE_BINDING"
    with __import__("pytest").raises(ModuleNotFoundError):
        importlib.import_module("hfa.governance.quota_manager")


def test_admission_preserves_grouped_legacy_optional_import_contract():
    source = (repo_root() / "hfa-control/src/hfa_control/admission.py").read_text()
    grouped = source.split("try:", 1)[1].split("except ImportError:", 1)[0]
    assert "hfa.governance.quota_manager" in grouped
    assert "hfa_tools.middleware.tenant" in grouped
    assert "canonical_validate_run_id_format" in source


def test_built_wheel_contains_both_admission_lua_resources(tmp_path):
    root = repo_root()
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    package_source = isolated_hfa_core_copy(root, tmp_path)
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
            str(package_source),
        ],
        check=True,
        cwd=tmp_path,
    )
    wheels = list(wheel_dir.glob("hfa_core-*.whl"))
    assert len(wheels) == 1
    with zipfile.ZipFile(wheels[0]) as archive:
        names = set(archive.namelist())
    assert "hfa/lua/admission_resource_reservation.lua" in names
    assert "hfa/lua/run_create_projection.lua" in names


def test_projection_path_resolves_from_isolated_wheel_install(tmp_path, monkeypatch):
    root = repo_root()
    wheel_dir = tmp_path / "wheel"
    target = tmp_path / "target"
    wheel_dir.mkdir()
    target.mkdir()
    package_source = isolated_hfa_core_copy(root, tmp_path)
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
            str(package_source),
        ],
        check=True,
        cwd=tmp_path,
    )
    wheel = next(wheel_dir.glob("hfa_core-*.whl"))
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
        cwd=tmp_path,
    )
    module = importlib.import_module("hfa_control.run_create_authority")
    monkeypatch.setattr(
        module,
        "__file__",
        str(target / "hfa_control" / "run_create_authority.py"),
    )
    resolved = module._projection_lua_path()
    assert resolved == target / "hfa/lua/run_create_projection.lua"
    assert resolved.read_text().startswith("-- Sprint 84.4")

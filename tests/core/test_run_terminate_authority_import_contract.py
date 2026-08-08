from __future__ import annotations

import importlib
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]



def isolated_package_copy(root: Path, package_name: str, tmp_path: Path) -> Path:
    source_root = tmp_path / "source"
    source_root.mkdir(exist_ok=True)
    destination = source_root / package_name
    shutil.copytree(
        root / package_name,
        destination,
        ignore=shutil.ignore_patterns("build", "__pycache__", ".pytest_cache"),
    )
    return destination

def test_run_terminate_authority_module_imports_without_worker_composition_changes():
    module = importlib.import_module("hfa_control.run_terminate_authority")
    assert module.WRITER_ID == "hfa-control/run-terminate-writer:v1"
    assert module.PROOF_SCHEMA_VERSION == 1


def test_legacy_run_terminate_lua_is_unchanged_projection_source():
    source = (repo_root() / "hfa-control/src/hfa_control/run_termination.py").read_text()
    assert '_lua_path("run_terminate_from_tasks.lua")' in source
    assert "authority_binding" in source


def test_built_hfa_core_wheel_contains_run_terminate_lua_resources(tmp_path):
    root = repo_root()
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    package_source = isolated_package_copy(root, "hfa-core", tmp_path)
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
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    assert "hfa/lua/run_terminate_terminal_proof.lua" in names
    assert "hfa/lua/run_terminate_projection.lua" in names


def test_built_hfa_control_wheel_contains_authority_adapter(tmp_path):
    root = repo_root()
    wheel_dir = tmp_path / "wheel"
    wheel_dir.mkdir()
    package_source = isolated_package_copy(root, "hfa-control", tmp_path)
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
    wheel = next(wheel_dir.glob("hfa_control-*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        names = set(archive.namelist())
    assert "hfa_control/run_terminate_authority.py" in names


def test_lua_paths_resolve_from_isolated_hfa_core_wheel_install(tmp_path, monkeypatch):
    root = repo_root()
    wheel_dir = tmp_path / "wheel"
    target = tmp_path / "target"
    wheel_dir.mkdir()
    target.mkdir()
    package_source = isolated_package_copy(root, "hfa-core", tmp_path)
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
    module = importlib.import_module("hfa_control.run_terminate_authority")
    monkeypatch.setattr(
        module,
        "__file__",
        str(target / "hfa_control" / "run_terminate_authority.py"),
    )
    proof = module._lua_path("run_terminate_terminal_proof.lua")
    projection = module._lua_path("run_terminate_projection.lua")
    assert proof == target / "hfa/lua/run_terminate_terminal_proof.lua"
    assert projection == target / "hfa/lua/run_terminate_projection.lua"
    assert proof.read_text().startswith("-- Sprint 84.5")
    assert projection.read_text().startswith("-- Sprint 84.6")

#!/usr/bin/env python
"""Compatibility wrapper for direct-write CI checks.

This wrapper keeps the command name explicit while delegating to the broader
read-only authority audit tool.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_authority_audit():
    script = Path(__file__).resolve().parent / "authority_audit.py"
    spec = importlib.util.spec_from_file_location("authority_audit", script)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load authority_audit.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    authority_audit = _load_authority_audit()
    args = list(argv if argv is not None else sys.argv[1:])
    if "--fail-on" not in args:
        args.extend(["--fail-on", "banned"])
    return int(authority_audit.main(args))


if __name__ == "__main__":
    raise SystemExit(main())

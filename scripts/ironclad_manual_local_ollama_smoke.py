from __future__ import annotations

import argparse
import json
import os
import subprocess
from typing import Any

DEFAULT_MODEL = "llama3.2:1b"
DEFAULT_PROMPT = "Return exactly: IRONCLAD_LOCAL_SMOKE_OK"
DEFAULT_ALLOWED_MODELS = {"llama3.2:1b"}


def _csv_set(value: str | None, default: set[str]) -> set[str]:
    if not value or not value.strip():
        return set(default)
    return {part.strip().lower() for part in value.split(",") if part.strip()}



def _enabled(env: dict[str, str] | None = None) -> bool:
    source = env if env is not None else os.environ
    return (source.get("IRONCLAD_LOCAL_OLLAMA_SMOKE") or "").strip() == "1"


def build_local_ollama_smoke_artifact(
    env: dict[str, str] | None = None,
    *,
    execute: bool = False,
    runner: Any | None = None,
) -> dict[str, Any]:
    source = env if env is not None else os.environ
    model = (source.get("IRONCLAD_OLLAMA_MODEL") or DEFAULT_MODEL).strip()
    enabled = _enabled(source)
    allowed_models = _csv_set(source.get("IRONCLAD_ALLOWED_OLLAMA_MODELS"), DEFAULT_ALLOWED_MODELS)
    model_allowed = model.lower() in allowed_models

    artifact: dict[str, Any] = {
        "source": "manual_local_ollama_smoke_gate",
        "status": "READY" if enabled else "BLOCKED",
        "manual_only": True,
        "ci_safe": True,
        "provider": "ollama",
        "model": model,
        "manual_local_ollama_smoke_supported": True,
        "manual_local_ollama_smoke_ready": bool(enabled and model_allowed),
        "local_only": True,
        "ollama_cli_required": True,
        "allowed_models": sorted(allowed_models),
        "model_allowed": model_allowed,
        "local_model_call_attempted": False,
        "network_call_attempted": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "prompt_value_exposed": False,
        "prompt_length": len(DEFAULT_PROMPT),
        "output_text_present": False,
        "output_text_value_exposed": False,
        "stderr_value_exposed": False,
        "blocked_reason": None if enabled else "IRONCLAD_LOCAL_OLLAMA_SMOKE=1 is required",
        "failing_reasons": [],
    }

    if not model_allowed:
        artifact["status"] = "BLOCKED"
        artifact["blocked_reason"] = f"ollama model {model!r} is not allowlisted"
        return artifact

    if not enabled:
        return artifact

    if not execute:
        artifact["status"] = "READY"
        artifact["blocked_reason"] = "local Ollama smoke ready; pass --execute to run local model"
        return artifact

    artifact["local_model_call_attempted"] = True
    cmd = ["ollama", "run", model, DEFAULT_PROMPT]

    try:
        if runner is not None:
            completed = runner(cmd)
        else:
            completed = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=float(source.get("IRONCLAD_OLLAMA_TIMEOUT_SECONDS", "60")),
            )
    except Exception as exc:
        artifact["status"] = "FAILED"
        artifact["error_type"] = type(exc).__name__
        artifact["error_message_exposed"] = False
        artifact["failing_reasons"] = ["local Ollama smoke execution failed"]
        return artifact

    returncode = getattr(completed, "returncode", 1)
    stdout = getattr(completed, "stdout", "") or ""
    stderr = getattr(completed, "stderr", "") or ""

    artifact["ollama_returncode"] = returncode
    artifact["output_text_present"] = bool(stdout.strip())
    artifact["stderr_present"] = bool(stderr.strip())

    if returncode == 0 and "IRONCLAD_LOCAL_SMOKE_OK" in stdout:
        artifact["status"] = "PASS"
        artifact["blocked_reason"] = None
    else:
        artifact["status"] = "FAILED"
        artifact["failing_reasons"] = ["local Ollama smoke did not return expected marker"]

    return artifact


def main() -> int:
    parser = argparse.ArgumentParser(description="Manual-only local Ollama smoke gate.")
    parser.add_argument("--json", action="store_true", help="Print JSON artifact.")
    parser.add_argument("--execute", action="store_true", help="Run local Ollama model if enabled.")
    args = parser.parse_args()

    artifact = build_local_ollama_smoke_artifact(execute=args.execute)

    if args.json:
        print(json.dumps(artifact, indent=2, sort_keys=True))
    else:
        print(
            f"status={artifact['status']} provider=ollama "
            f"local_model_call_attempted={artifact['local_model_call_attempted']} "
            f"network_call_attempted={artifact['network_call_attempted']} "
            f"production_llm_call_attempted={artifact['production_llm_call_attempted']}"
        )

    return 0 if artifact["status"] in {"BLOCKED", "READY", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

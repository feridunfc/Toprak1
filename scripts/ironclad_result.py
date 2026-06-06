from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ARTIFACT_PATH = Path("docs/dashboard/artifacts/latest_thin_product_task_cli_demo.json")


def _json_print(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _read_result(run_id: str) -> dict[str, Any]:
    # Thin product result command.
    #
    # For Sprint 43, the product-visible persisted result comes from the demo
    # artifact written by ironclad_demo.py. This avoids inventing a second ad-hoc
    # result store while keeping the user-visible command useful.
    if not ARTIFACT_PATH.exists():
        return {
            "source": "ironclad_result",
            "status": "NOT_FOUND_OR_NOT_PERSISTED",
            "run_id": run_id,
            "result_readable": False,
            "failing_reasons": [
                "thin product demo artifact not found; run scripts/ironclad_demo.py first"
            ],
            "production_llm_call_attempted": False,
            "deployment_attempted": False,
            "release_tag_created": False,
            "operator_action_buttons": False,
        }

    artifact = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))

    artifact_run_id = artifact.get("run_id") or artifact.get("submitted", {}).get("run_id")
    if artifact_run_id != run_id:
        return {
            "source": "ironclad_result",
            "status": "NOT_FOUND_OR_NOT_PERSISTED",
            "run_id": run_id,
            "result_readable": False,
            "available_run_id": artifact_run_id,
            "failing_reasons": ["requested run_id does not match latest thin product demo artifact"],
            "production_llm_call_attempted": False,
            "deployment_attempted": False,
            "release_tag_created": False,
            "operator_action_buttons": False,
        }

    return {
        "source": "ironclad_result",
        "status": "COMPLETED" if artifact.get("result_readable") else "NOT_COMPLETED",
        "tenant_id": artifact.get("tenant_id"),
        "task_id": artifact.get("task_id"),
        "run_id": run_id,
        "result_readable": bool(artifact.get("result_readable")),
        "result": artifact.get("result"),
        "runtime_claim": artifact.get("runtime_claim"),
        "uses_sprint_42_runtime": artifact.get("uses_sprint_42_runtime", False),
        "production_llm_call_attempted": artifact.get("production_llm_call_attempted", False),
        "deployment_attempted": artifact.get("deployment_attempted", False),
        "release_tag_created": artifact.get("release_tag_created", False),
        "operator_action_buttons": artifact.get("operator_action_buttons", False),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read an IRONCLAD task result by run id.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--redis-url", default="redis://localhost:6389/0")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    args = parser.parse_args(argv)

    payload = _read_result(args.run_id)

    if args.json:
        _json_print(payload)
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))

    return 0 if payload["status"] in {"COMPLETED", "NOT_FOUND_OR_NOT_PERSISTED", "NOT_COMPLETED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "docs" / "dashboard" / "artifacts"

STORE_PATH = ARTIFACT_DIR / "task_execution_product_store.json"
DEMO_OUTPUT_PATH = ARTIFACT_DIR / "latest_task_execution_demo.json"

ALLOWED_TASK_TYPES = {"echo"}
SAFE_EXECUTOR = "EchoExecutor"
FALLBACK_MODE = "artifact_backed_safe_local_adapter"

LIFECYCLE_SUBMITTED = "SUBMITTED"
LIFECYCLE_QUEUED = "QUEUED"
LIFECYCLE_CLAIMED = "CLAIMED"
LIFECYCLE_EXECUTED = "EXECUTED"
LIFECYCLE_COMPLETED = "COMPLETED"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_store() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"tasks": []}

    try:
        data = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"tasks": []}

    if not isinstance(data, dict):
        return {"tasks": []}

    tasks = data.get("tasks")
    if not isinstance(tasks, list):
        return {"tasks": []}

    return {"tasks": tasks}


def _write_store(store: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STORE_PATH.write_text(
        json.dumps(store, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _safe_payload(payload: str) -> dict[str, Any]:
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("payload must be a JSON object")
    return data


def _base_safety() -> dict[str, Any]:
    return {
        "executor": SAFE_EXECUTOR,
        "fallback_mode": FALLBACK_MODE,
        "canonical_task_lifecycle_mutation": True,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "noncanonical_redis_mutation_attempted": False,
        "requeue_attempted": False,
        "auto_resume_attempted": False,
        "operator_action_buttons": False,
        "actionable": False,
        "actions": [],
    }


def submit_task(tenant_id: str, task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not tenant_id:
        raise ValueError("tenant_id is required")
    if task_type not in ALLOWED_TASK_TYPES:
        raise ValueError(f"unsupported task type: {task_type}")

    task_id = f"task_{uuid.uuid4().hex}"
    run_id = f"run_{uuid.uuid4().hex}"
    now = _now()

    task = {
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "task_type": task_type,
        "payload": payload,
        "status": LIFECYCLE_QUEUED,
        "lifecycle": [
            LIFECYCLE_SUBMITTED,
            LIFECYCLE_QUEUED,
        ],
        "result": None,
        "created_at": now,
        "updated_at": now,
        **_base_safety(),
    }

    store = _load_store()
    store["tasks"].append(task)
    _write_store(store)

    return {
        "status": LIFECYCLE_SUBMITTED,
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "task_type": task_type,
        **_base_safety(),
    }


def _execute_echo(payload: dict[str, Any]) -> dict[str, Any]:
    message = payload.get("message")
    return {"echo": message}


def run_worker_once(tenant_id: str) -> dict[str, Any]:
    store = _load_store()
    tasks = store["tasks"]

    selected: dict[str, Any] | None = None
    for task in tasks:
        if task.get("tenant_id") == tenant_id and task.get("status") == LIFECYCLE_QUEUED:
            selected = task
            break

    if selected is None:
        return {
            "status": "NO_TASK_AVAILABLE",
            "tenant_id": tenant_id,
            **_base_safety(),
        }

    selected["status"] = LIFECYCLE_CLAIMED
    selected["lifecycle"].append(LIFECYCLE_CLAIMED)
    selected["updated_at"] = _now()

    if selected["task_type"] == "echo":
        result = _execute_echo(selected["payload"])
    else:
        raise ValueError(f"unsupported task type: {selected['task_type']}")

    selected["lifecycle"].append(LIFECYCLE_EXECUTED)
    selected["result"] = result
    selected["status"] = LIFECYCLE_COMPLETED
    selected["lifecycle"].append(LIFECYCLE_COMPLETED)
    selected["updated_at"] = _now()

    _write_store(store)

    return {
        "status": LIFECYCLE_COMPLETED,
        "tenant_id": selected["tenant_id"],
        "task_id": selected["task_id"],
        "run_id": selected["run_id"],
        "task_type": selected["task_type"],
        "result": result,
        "lifecycle": selected["lifecycle"],
        **_base_safety(),
    }


def get_task_result(run_id: str) -> dict[str, Any]:
    store = _load_store()
    for task in store["tasks"]:
        if task.get("run_id") == run_id:
            return {
                "status": task.get("status"),
                "tenant_id": task.get("tenant_id"),
                "task_id": task.get("task_id"),
                "run_id": task.get("run_id"),
                "task_type": task.get("task_type"),
                "result": task.get("result"),
                "lifecycle": task.get("lifecycle", []),
                **_base_safety(),
            }

    return {
        "status": "NOT_FOUND",
        "run_id": run_id,
        **_base_safety(),
    }


def write_demo_artifact(result: dict[str, Any]) -> dict[str, Any]:
    artifact = {
        "source": "task_execution_demo",
        "status": "PASS" if result.get("status") == LIFECYCLE_COMPLETED else "FAIL",
        "tenant_id": result.get("tenant_id"),
        "task_id": result.get("task_id"),
        "run_id": result.get("run_id"),
        "task_type": result.get("task_type"),
        "lifecycle": result.get("lifecycle", []),
        "result": result.get("result"),
        "fallback_mode": FALLBACK_MODE,
        "fallback_reason": (
            "No stable repository task submit/worker entrypoint was found during Sprint 37A discovery."
        ),
        **_base_safety(),
    }

    DEMO_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEMO_OUTPUT_PATH.write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifact


def run_demo(tenant_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    submitted = submit_task(tenant_id=tenant_id, task_type="echo", payload=payload)
    completed = run_worker_once(tenant_id=tenant_id)
    result = get_task_result(completed["run_id"])
    artifact = write_demo_artifact(result)

    return {
        "source": "task_execution_product_path",
        "status": "PASS" if artifact["status"] == "PASS" else "FAIL",
        "submitted": submitted,
        "completed": completed,
        "result": result,
        "artifact": artifact,
    }


def main_args(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Safe end-to-end tenant task execution product path."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit")
    submit.add_argument("--tenant", required=True)
    submit.add_argument("--type", required=True)
    submit.add_argument("--payload", required=True)

    worker = sub.add_parser("run-worker-once")
    worker.add_argument("--tenant", required=True)

    result = sub.add_parser("get-result")
    result.add_argument("--run-id", required=True)

    demo = sub.add_parser("demo")
    demo.add_argument("--tenant", default="demo")
    demo.add_argument("--payload", default=None)
    demo.add_argument("--message", default="hello")

    args = parser.parse_args(argv)

    if args.command == "submit":
        output = submit_task(args.tenant, args.type, _safe_payload(args.payload))
    elif args.command == "run-worker-once":
        output = run_worker_once(args.tenant)
    elif args.command == "get-result":
        output = get_task_result(args.run_id)
    elif args.command == "demo":
        payload = _safe_payload(args.payload) if args.payload else {"message": args.message}
        output = run_demo(args.tenant, payload)
    else:
        raise AssertionError(args.command)

    print(json.dumps(output, indent=2, sort_keys=True))
    return 0 if output.get("status") in {"PASS", LIFECYCLE_SUBMITTED, LIFECYCLE_COMPLETED, "NO_TASK_AVAILABLE", "NOT_FOUND"} else 1


if __name__ == "__main__":
    raise SystemExit(main_args())


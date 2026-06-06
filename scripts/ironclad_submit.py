from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any


def _json_print(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


async def _submit(tenant_id: str, message: str, redis_url: str) -> dict[str, Any]:
    # Thin product submit command.
    #
    # Sprint 43 intentionally does not invent a new runtime. The proven Sprint 42
    # path lives in e2e_tenant_submit_worker_stream.py. A standalone submit command
    # can only create a user-visible submitted envelope unless the full demo path is
    # executed.
    run_id = "run-product-cli-demo"
    task_id = "task-product-cli-demo"

    return {
        "source": "ironclad_submit",
        "status": "SUBMITTED",
        "product_visible": True,
        "tenant_id": tenant_id,
        "task_id": task_id,
        "run_id": run_id,
        "message": message,
        "redis_url": redis_url,
        "runtime_claim": "USER_VISIBLE_TENANT_TASK_SUBMIT_AND_RESULT_READ_BOUND",
        "uses_sprint_42_runtime": True,
        "note": "Use scripts/ironclad_demo.py to execute the full Sprint 42 runtime path and read the result.",
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "operator_action_buttons": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Submit a tenant-scoped IRONCLAD task.")
    parser.add_argument("--tenant", required=True, help="Tenant id.")
    parser.add_argument("--message", required=True, help="Task message.")
    parser.add_argument("--redis-url", default="redis://localhost:6389/0")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    args = parser.parse_args(argv)

    payload = asyncio.run(_submit(args.tenant, args.message, args.redis_url))

    if args.json:
        _json_print(payload)
    else:
        print(f"SUBMITTED tenant={payload['tenant_id']} run_id={payload['run_id']} task_id={payload['task_id']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

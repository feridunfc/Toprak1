from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.runtime_alpha_acceptance_83_8 import (
    build_acceptance_report,
)

from tests.acceptance_fresh_domain import (  # noqa: F401
    integration_redis_stack, strict_mode_env, flush_redis, owned_domain, prepared_product, product_cli_environment,
)


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


async def test_idempotent_submission_report_83_8(prepared_product):
    grant, issuer = await prepared_product(build_acceptance_report,
        scenario="s83-8", acceptance_id="s83-8-integration")
    report = await build_acceptance_report(
        prepared_grant=grant, prepared_issuer=issuer,
        acceptance_id="s83-8-integration",
    )

    assert report["schema_version"] == 1
    assert report["sprint"] == "83.8"
    assert report["status"] == (
        "PASS_WITH_LIMITATIONS"
    )
    assert report["real_redis_used"] is True
    assert report[
        "real_asgi_application_used"
    ] is True
    assert report[
        "production_control_composition_used"
    ] is True
    assert report[
        "submission_idempotency_supported"
    ] is True
    assert report[
        "same_key_same_request_replay"
    ] is True
    assert report[
        "same_key_different_request_conflict"
    ] is True
    assert report[
        "concurrent_in_progress_fail_closed"
    ] is True
    assert report[
        "tenant_scoped_namespace"
    ] is True
    assert report[
        "missing_key_fail_closed"
    ] is True
    assert report[
        "invalid_key_fail_closed"
    ] is True
    assert report[
        "corrupt_evidence_fail_closed"
    ] is True
    assert report[
        "composition_restart_durability"
    ] is True
    assert report[
        "zero_replay_lifecycle_writes"
    ] is True
    assert report[
        "raw_idempotency_key_persisted"
    ] is False
    assert report["retention_seconds"] == 86_400

    assert report["multi_task_support"] is False
    assert report["cancel_supported"] is False
    assert report["retry_supported"] is False
    assert report["automatic_repair"] is False
    assert report["stale_owner_takeover"] is False
    assert report[
        "external_executor_cutover"
    ] is False
    assert report["production_ready"] is False
    assert report[
        "production_cutover_authorized"
    ] is False
    assert report["new_lifecycle_writer"] is False

    scenario = report["scenarios"][0]
    assert scenario["status"] == "PASS"
    assert scenario["first_submission"][
        "status_code"
    ] == 202
    assert scenario["first_submission"][
        "idempotent_replay"
    ] is False
    assert scenario["in_progress_replay"][
        "status_code"
    ] == 409
    assert scenario["in_progress_replay"][
        "failure_code"
    ] == "IDEMPOTENCY_IN_PROGRESS"
    assert scenario["final_replay"][
        "status_code"
    ] == 200
    assert scenario["final_replay"][
        "idempotent_replay"
    ] is True
    assert scenario["final_replay"][
        "same_run_id"
    ] is True
    assert scenario["final_replay"][
        "same_task_id"
    ] is True
    assert scenario["final_replay"][
        "zero_keyspace_mutation"
    ] is True
    assert scenario["payload_conflict"][
        "status_code"
    ] == 409
    assert scenario["payload_conflict"][
        "failure_code"
    ] == "IDEMPOTENCY_KEY_REUSED"
    assert scenario["payload_conflict"][
        "zero_keyspace_mutation"
    ] is True
    assert scenario["tenant_namespace"][
        "distinct_redis_keys"
    ] is True
    assert scenario["tenant_namespace"][
        "distinct_run_ids"
    ] is True
    assert scenario["validation"][
        "missing_key_status_code"
    ] == 400
    assert scenario["validation"][
        "invalid_key_status_code"
    ] == 400
    assert scenario["validation"][
        "zero_keyspace_mutation"
    ] is True
    assert scenario["corrupt_evidence"][
        "status_code"
    ] == 503
    assert scenario["corrupt_evidence"][
        "failure_code"
    ] == "IDEMPOTENCY_STORE_FAILED"
    assert scenario["corrupt_evidence"][
        "zero_keyspace_mutation"
    ] is True
    assert scenario["durability"][
        "composition_restart_replay_status"
    ] == 200
    assert scenario["durability"][
        "same_run_id"
    ] is True
    assert scenario["durability"][
        "same_task_id"
    ] is True
    assert scenario["durability"][
        "zero_keyspace_mutation"
    ] is True
    assert scenario["retention"][
        "aligned_with_run_result"
    ] is True
    assert scenario["architecture"][
        "new_lifecycle_writer"
    ] is False
    assert scenario["architecture"][
        "automatic_repair_used"
    ] is False
    assert scenario["architecture"][
        "stale_owner_takeover_used"
    ] is False


async def test_idempotent_submission_cli_83_8(
    tmp_path,
):
    root = Path(__file__).resolve().parents[2]
    output = (
        tmp_path
        / "sprint83_8_idempotent_submission.json"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(
                root
                / "scripts"
                / "runtime_alpha_acceptance_83_8.py"
            ),
            "--owned-fresh-domain",
            "--acceptance-id",
            "s83-8-cli",
            "--out",
            str(output),
            "--json",
        ],
        cwd=root,
        env=product_cli_environment(),
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )

    assert completed.returncode == 0, (
        completed.stdout + completed.stderr
    )
    report = json.loads(
        output.read_text(encoding="utf-8")
    )
    assert report["status"] == (
        "PASS_WITH_LIMITATIONS"
    )
    assert report[
        "submission_idempotency_supported"
    ] is True
    assert report[
        "composition_restart_durability"
    ] is True
    assert report["production_ready"] is False
    assert report["scenarios"][0][
        "status"
    ] == "PASS"

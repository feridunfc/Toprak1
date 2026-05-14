
from hfa.dag.reasons import TASK_CLAIMED, TASK_ALREADY_OWNED, TASK_RUNNING_STATE_CONFLICT
from hfa.dag.schema import DagRedisKey


def test_task_claim_reason_constants_exist():
    assert TASK_CLAIMED == "task_claimed"
    assert TASK_ALREADY_OWNED == "task_already_owned"
    assert TASK_RUNNING_STATE_CONFLICT == "task_running_state_conflict"


def test_task_claim_key_shapes():
    assert DagRedisKey.task_state("t1").endswith(":state")
    assert DagRedisKey.task_meta("t1").endswith(":meta")
    assert DagRedisKey.task_running_zset("tenant-a").endswith(":running")

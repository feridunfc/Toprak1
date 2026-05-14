from hfa.dag.schema import DagRedisKey


def test_dag_completion_key_shapes():
    assert DagRedisKey.task_state("t1") == "hfa:dag:task:t1:state"
    assert DagRedisKey.task_meta("t1") == "hfa:dag:task:t1:meta"
    assert DagRedisKey.task_children("t1") == "hfa:dag:task:t1:children"
    assert DagRedisKey.task_remaining_deps("t2") == "hfa:dag:task:t2:remaining_deps"
    assert DagRedisKey.tenant_ready_queue("tenant") == "hfa:dag:tenant:tenant:ready"

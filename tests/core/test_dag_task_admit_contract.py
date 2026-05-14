
from hfa.dag.schema import DagRedisKey, DagTaskSeed


def test_dag_key_shapes():
    assert DagRedisKey.task_meta('t1') == 'hfa:dag:task:t1:meta'
    assert DagRedisKey.task_ready_queue('tenant-a') == 'hfa:dag:tenant:tenant-a:ready'


def test_task_seed_defaults():
    task = DagTaskSeed(task_id='t1', tenant_id='tenant-a', run_id='run-1', agent_type='default', priority=5, admitted_at=1.0)
    assert task.dependency_count == 0
    assert task.child_task_ids == ()

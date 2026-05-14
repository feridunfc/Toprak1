from hfa.dag.schema import DagRedisKey, DagTaskDispatchInput


def test_dag_dispatch_key_shapes():
    assert DagRedisKey.task_running_zset('tenant-a') == 'hfa:dag:tenant:tenant-a:running'
    assert DagRedisKey.task_state('task-1') == 'hfa:dag:task:task-1:state'


def test_dispatch_input_carries_stream_and_running_targets():
    item = DagTaskDispatchInput(
        task_id='task-1',
        run_id='run-1',
        tenant_id='tenant-a',
        agent_type='default',
        worker_group='grp-a',
        shard=0,
        priority=5,
        admitted_at=1.0,
        scheduled_at=2.0,
        running_zset='hfa:dag:tenant:tenant-a:running',
        control_stream='hfa:stream:control',
        shard_stream='hfa:stream:runs:0',
    )
    assert item.running_zset.endswith(':running')
    assert item.control_stream.endswith(':control')

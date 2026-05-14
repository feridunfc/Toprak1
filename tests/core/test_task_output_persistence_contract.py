
from hfa.dag.schema import DagRedisKey

def test_task_output_key_shape():
    assert DagRedisKey.task_output("task-123") == "hfa:dag:task:task-123:output"

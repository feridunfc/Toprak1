
def test_task_claim_manager_exists():
    from hfa_control.task_claim import TaskClaimManager, TaskClaimService
    assert issubclass(TaskClaimManager, TaskClaimService)


def test_reservation_dispatch_result_exists():
    from hfa_control.scheduler_reservation_dispatch import (
        ReservationDispatchResult,
        SchedulerReservationDispatchResult,
    )
    assert ReservationDispatchResult is SchedulerReservationDispatchResult


def test_dag_schema_contracts_exist():
    from hfa.dag.schema import DagTaskSeed, DagTaskDispatchInput, DagRedisKey

    seed = DagTaskSeed(task_id="t1", run_id="r1", tenant_id="tenant-a")
    dispatch = DagTaskDispatchInput(task_id="t1", run_id="r1", tenant_id="tenant-a")

    assert seed.task_id == "t1"
    assert dispatch.run_id == "r1"
    assert DagRedisKey.task_state("t1") == "hfa:dag:task:t1:state"

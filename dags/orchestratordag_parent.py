from dag_orchestrator_1 import first_orchestrator_dag
from dag_orchestrator_2 import second_orchestrator_dag
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag
def orchestrator_parent_dag():
    # Trigger the first orchestrator DAG
    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_orchestrator_dag",
        trigger_dag_id="first_orchestrator_dag",
        wait_for_completion=True,
    )

    # Trigger the second orchestrator DAG
    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_orchestrator_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True,
    )

    # Define the task dependencies
    trigger_first_dag >> trigger_second_dag

# Instantiate the parent DAG
orchestrator_parent_dag()
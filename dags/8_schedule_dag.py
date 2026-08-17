from airflow.sdk import dag, task
from pendulum import datetime

@dag(
        dag_id="schedule_dag",
        start_date=datetime(year=2026, month=7, day=12, tz="Asia/Kolkata"),
        schedule="@daily",
        end_date=datetime(year=2026, month=7, day=20, tz="Asia/Kolkata"),
        is_paused_upon_creation=False
)
def schedule_dag():

    @task.python
    def first_task():
        print("This is the first task")
    
    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    # Define the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#instantiate the DAG
schedule_dag()


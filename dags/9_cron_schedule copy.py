from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
        dag_id="cron_schedule_copy_dag",
        start_date=datetime(year=2026, month=7, day=5, tz="Asia/Kolkata"),
        schedule="0 14 * * 1-5",
        end_date=datetime(year=2026, month=7, day=20, tz="Asia/Kolkata"),
        is_paused_upon_creation=False,
        catchup=True

)
def cron_schedule_copy_dag():

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
cron_schedule_copy_dag()


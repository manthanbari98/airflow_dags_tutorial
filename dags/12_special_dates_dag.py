from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable


special_dates = EventsTimetable([
    datetime(2026,7,1),
    datetime(2026,7,10),
    datetime(2026,7,18),
    datetime(2026,7,20)
])

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=7, day=1, tz="Asia/Kolkata"),
    end_date=datetime(year=2026, month=7, day=30, tz="Asia/Kolkata"),
    catchup=True,
)

def special_dates_dag():
    @task.python
    def print_special_dates(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Running DAG for special date: {execution_date}")

#instantiate the DAG
special_dates_dag()
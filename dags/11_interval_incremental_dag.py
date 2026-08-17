from airflow.sdk import dag,task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule = CronDataIntervalTimetable("@daily", timezone="Asia/Kolkata"),
    start_date = datetime(year=2026, month=7, day=12, tz="Asia/Kolkata"),
    end_date = datetime(year=2026, month=7, day=20, tz="Asia/Kolkata"),
    catchup=True
)
def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start'].in_tz("Asia/Kolkata")
        date_interval_end = kwargs['data_interval_end'].in_tz("Asia/Kolkata")
        print(f"Fetching data from :{date_interval_start} to {date_interval_end}")
    
    @task.python
    def incremental_data_start(**kwargs):
        print("logical_date:", kwargs["logical_date"])
        print("data_interval_start:", kwargs["data_interval_start"])
        print("data_interval_end:", kwargs["data_interval_end"])
        print("dag.timezone:", kwargs["dag"].timezone)

    @task.bash
    def incremental_data_processing():
        return "echo 'Processing incremental data from {{data_interval_start.in_timezone('Asia/Kolkata') }} to {{data_interval_end.in_timezone('Asia/Kolkata') }}'"

    fetch_task = incremental_data_fetch()
    start_task = incremental_data_start()
    process_task = incremental_data_processing()
    fetch_task >> start_task >> process_task

#instantiate the DAG
incremental_load_dag()
from airflow.sdk import dag, task

@dag(
        dag_id="parallel_dag",
)
def parallel_dag():

    @task.python
    def extract_task(**kwargs):
        print("This is the extract task")
        ti = kwargs['ti']
        extract_data_dict = {"api_extract_data": [1,2,3],
                             "db_extract_data": [4,5,6],
                             "s3_extract_data": [7,8,9]}
        ti.xcom_push(key='return_value', value=extract_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        print("This is the api transform task")
        ti = kwargs['ti']
        api_extract_data = ti.xcom_pull(task_ids='extract_task')['api_extract_data']
        transformed_api_data = [i*10 for i in api_extract_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        print("This is the db transform task")
        ti = kwargs['ti']
        db_extract_data = ti.xcom_pull(task_ids='extract_task')['db_extract_data']
        transformed_db_data = [i*10 for i in db_extract_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        print("This is the s3 transform task")
        ti = kwargs['ti']
        s3_extract_data = ti.xcom_pull(task_ids='extract_task')['s3_extract_data']
        transformed_s3_data = [i*10 for i in s3_extract_data]
        ti.xcom_push(key='return_value', value=transformed_s3_data)
        
    @task.bash
    def load_task(**kwargs):
        print("This is the load task")
        api_data = kwargs['ti'].xcom_pull(task_ids='transform_task_api')
        db_data = kwargs['ti'].xcom_pull(task_ids='transform_task_db')
        s3_data = kwargs['ti'].xcom_pull(task_ids='transform_task_s3')
        return f"echo 'Loading data: {api_data}, {db_data}, {s3_data}'"


    # Define the task dependencies
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()

    extract >> [transform_api, transform_db, transform_s3] >> load

#instantiate the DAG
parallel_dag()

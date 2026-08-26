from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, date, timedelta, timezone

@dag
def kwargs_dag():

    @task.python(retries=3,retry_delay=timedelta(seconds=5))
    def fetch_data(**kwargs):
        ti=kwargs['ti']
        print("Printing Kwargs : ",kwargs)

        data={"name":"Airflow","Version":"2.0","Company":"Hitbits9"}
        ti.xcom_push(key='fetched_data',value=data)
        
      
        
    @task.python(retries=3,retry_delay=timedelta(seconds=5))
    def process_data(**kwargs):
        ti=kwargs['ti']
        
        #pull the dtaa
        pulled_data=ti.xcom_pull(key='fetched_data',task_ids="fetch_data")


        #stimulate processing data
        process_data=f"Processed {pulled_data['name']} version {pulled_data['Version']} company {pulled_data['Company']}"
        print(process_data)

    @task.bash(retries=3,retry_delay=timedelta(seconds=5))
    def bash_task(**kwargs):
        ti=kwargs['ti']
        print("Logical date : ",kwargs['logical_date'])
        return "echo 'This is the bash task!' fetched data is {{ti.xcom_pull(key='fetched_data',task_ids='fetch_data')}}"

    fetch_data()>>process_data()>>bash_task()

dag=kwargs_dag()

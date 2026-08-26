from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum

@dag(
        start_date=pendulum.datetime(year=2026,month=1,day=1,tz="Asia/Kolkata"),
        schedule="14 16 * * *",
        catchup=False,
        is_paused_upon_creation=False
)
def schedule_basic():

    @task.python
    def fetch_data(do_xcom_push:bool=True) -> dict: #this parameter is by default true

        data={"name":"Airflow","Version":"2.0","Company":"Hitbits9"}
       
        
        return data 
        #pushing some data to xcom manually
        
    @task.python
    def process_data(pulled_data:dict):
        #pull the dtaa
 


        #stimulate processing data
        process_data=f"Processed {pulled_data['name']} version {pulled_data['Version']} company {pulled_data['Company']}"
        print(process_data)

    bash_task=BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is the bash task!'"
    )
    pulled_data=fetch_data()

    process_data(pulled_data)>>bash_task

dag=schedule_basic()
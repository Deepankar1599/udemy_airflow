from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag
def xcoms_manual():

    @task.python
    def fetch_data(ti):

        data={"name":"Airflow","Version":"2.0","Company":"Hitbits9"}
        ti.xcom_push(key='fetched_data',value=data)
        
        return data 
        #pushing some data to xcom manually
        
    @task.python
    def process_data(ti):
        #pull the dtaa
        pulled_data=ti.xcom_pull(key='fetched_data',task_ids="fetch_data")


        #stimulate processing data
        process_data=f"Processed {pulled_data['name']} version {pulled_data['Version']} company {pulled_data['Company']}"
        print(process_data)

    bash_task=BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is the bash task!'"
    )

    fetch_data()>>process_data()>>bash_task

dag=xcoms_manual()
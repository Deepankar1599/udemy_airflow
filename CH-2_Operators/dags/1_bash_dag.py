from airflow.sdk import dag,task  # 1. Capitalised 'DAG'
from airflow.operators.bash import BashOperator

@dag(dag_id="bash_dag")  # 1. Capitalised 'DAG'
def bash_dag():

    @task.bash
    def first_task():
        return "echo 'Hello World!'"  # 2. Added space after 'echo'

    # Define the second task using Bash Operator
    second_task = BashOperator(
        task_id="second_task",
        bash_command="echo 'This is the second task'"  # 2. Added space after 'echo'
    )

    first_task_instance = first_task()
    first_task_instance >> second_task

# Create an instance of the DAG to register it
bash_operator_instance = bash_dag()

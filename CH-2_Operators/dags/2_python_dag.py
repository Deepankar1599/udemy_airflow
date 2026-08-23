from airflow.sdk import dag,task
from airflow.operators.python import PythonOperator

def first_task_fun():
    return "Hello World !"

def   second_task_fun():
    return "Hello World,from second task !"


@dag
def python_dag():

    first_task=PythonOperator(
        task_id="first_task",
        python_callable=first_task_fun
    )

    second_task=PythonOperator(
            task_id="second_task",
            python_callable=second_task_fun
    )

    first_task>>second_task

python_dag_instance=python_dag()

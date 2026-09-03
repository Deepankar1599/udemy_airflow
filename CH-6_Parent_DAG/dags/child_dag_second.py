from airflow.sdk import dag, task
import os


@dag
def child_dag_second_dag():

    @task
    def task_pre():
        print("TASK A")

    @task
    def task_write():

        os.makedirs("/tmp/data",exist_ok=True)

        #creating directory if it does not exist
        with open("/tmp/data/output_second.txt","w") as f:
            f.write("This is the Second child Dag")

    #Setting task dependencies
    task_pre()>>task_write()

dag_second_dag=child_dag_second_dag()


from airflow.sdk import dag, task
import os


@dag(is_paused_upon_creation=False)
def child_dag_first_dag():

    @task
    def task_pre():
        print("TASK A")

    @task
    def task_write():

        os.makedirs("/tmp/data",exist_ok=True)

        #creating directory if it does not exist
        with open("/tmp/data/output_first.txt","w") as f:
            f.write("This is the first child Dag")

    #Setting task dependencies
    task_pre()>>task_write()

dag_first_dag=child_dag_first_dag()


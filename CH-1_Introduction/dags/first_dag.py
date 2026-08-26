from airflow.sdk import dag,task

@dag(dag_id="FIRST_DAG")
def first_dag():

    @task(task_id="task_1")
    def task_1():
        print("This is task 1")

    @task(task_id="task_2")
    def task_2():
        print("This is the task 2")

    @task(task_id="task_3")
    def task_3():
        print("This is the task 3")

    #define the task dependenceies

    t1=task_1()
    t2=task_2()
    t3=task_3()

    t1>>t2>>t3 #this means task 1 will run before task 2

first_dag_instance=first_dag()
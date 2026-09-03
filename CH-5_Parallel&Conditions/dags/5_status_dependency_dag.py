from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule


@dag
def status_dependency_dag():

    @task.python
    def task_a():
        print("Executing Task A")
        return "echo 'Task A Completed'"

    @task.python
    def task_b():
        print("Executing Task B")
        raise Exception("Task B failed")
    @task.python
    def task_c():
        print("Executing Task C")
        return "echo 'Task C Completed'"

    @task.python(trigger_rule=TriggerRule.ALL_DONE)
    def task_d():
        print("Executing Task D")
        return "echo 'Task D Completed'"

    task_a=task_a()
    task_b=task_b()
    task_c=task_c()
    task_d=task_d()

    task_a>>[task_b,task_c]>>task_d

status_dependency=status_dependency_dag()
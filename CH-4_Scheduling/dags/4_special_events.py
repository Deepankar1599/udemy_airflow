from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum
from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.timetables.events import EventsTimetable
from pendulum import datetime
event_list=EventsTimetable(event_dates=[
    pendulum.datetime(year=2026,month=8,day=15,tz="Asia/Kolkata"),
    pendulum.datetime(year=2026,month=8,day=25,tz="Asia/Kolkata"),
    pendulum.datetime(year=2026,month=8,day=28,tz="Asia/Kolkata")
])

# event_list_obj=EventsTimetable(event_dates=[
#     datetime(2026,8,25),
#     datetime(2026,8,15),
#     datetime(2026,8,28)
    
# ])

@dag(
        schedule=event_list,
        start_date=pendulum.datetime(year=2026,month=8,day=24,tz="Asia/Kolkata"),
        catchup=False
)
def special_events():

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

dag=special_events()
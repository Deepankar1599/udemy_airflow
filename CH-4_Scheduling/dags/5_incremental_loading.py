from airflow.sdk import dag, task

from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum



# event_list_obj=EventsTimetable(event_dates=[
#     datetime(2026,8,25),
#     datetime(2026,8,15),
#     datetime(2026,8,28)
    
# ])

@dag(
        schedule=CronDataIntervalTimetable("0 0 * * *", timezone='Asia/Kolkata'),
        start_date=pendulum.datetime(year=2026,month=8,day=25,tz="Asia/Kolkata"),
        catchup=True
)
def incremental_loading():

    @task.python
    def extract_data(**kwargs): #this parameter is by default true

        from_date=kwargs['data_interval_start']
        to_date=kwargs['data_interval_end']

        #simulating exracting data from source
        print(f"Extracting data from {from_date} to {to_date}")
        print(f"select * from source_table where date >= {from_date} and date< {to_date}")
        
    @task.bash
    def load_data():
        #pull the dtaa

        return """
        echo "data loaded from {{data_interval_start | ds}} to {{data_interval_end | ds}}"
        """

    

    extract_data()>>load_data()

dag=incremental_loading()
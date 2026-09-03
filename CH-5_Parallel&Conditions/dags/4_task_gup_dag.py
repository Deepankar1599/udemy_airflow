from airflow.sdk import dag, task, task_group

@dag
def task_group_dag():

    @task.bash
    def bash_task():
       return "echo 'Hello Bash task here'"

    @task_group
    def fetch_task_group():

        @task.python
        def fetch_api():
            data={"type":"api","data":["data1","data2","data3"]}
            return data

        @task.python
        def fetch_db():
                data={"type":"db","data":["data4","data5","data6"]}
                return data

        @task.python
        def fetch_s3():
                data={"type":"s3","data":["data7","data8","data9"]}
                return data

        [fetch_api(),fetch_db(),fetch_s3()]
     #what ever your task function return will be sent through Xcom 
     # and key will be "return_value" here list of values in data


    @task.python
    def process_data(ti):
   
        api_data=ti.xcom_pull(task_ids='fetch_task_group.fetch_api',key='return_value')
        db_data=ti.xcom_pull(task_ids='fetch_task_group.fetch_db',key='return_value')
        s3_data=ti.xcom_pull(task_ids='fetch_task_group.fetch_s3',key='return_value')

        print("Processing the API Data: ",api_data)
        print("Processing the DB Data: ",db_data)
        print("Processing the S3 Data: ",s3_data)

        porcessed_data=api_data['data']+db_data['data']+s3_data['data']
        return porcessed_data

    @task.branch
    def load_data_branch(ti):
         #pulling data from previpous task

        processed_data=ti.xcom_pull(task_ids='process_data', key='return_value')
        if len(processed_data)>10:
             return 's3_load' #same name here for which youre craeting the function
        else:
             return 'glue_load'

    @task.python
    def s3_load(ti):
         data_to_load=ti.xcom_pull(task_ids='process_data', key='return_value')
         print("Loading data to s3: ",data_to_load)

    @task.python
    def glue_load(ti):
             data_to_load=ti.xcom_pull(task_ids='process_data', key='return_value')
             print("Loading data to Glue: ",data_to_load)

    

    bash_task() >> fetch_task_group() >> process_data()>>load_data_branch()>>[s3_load(),glue_load()]

    # load_data_branch()>>[s3_load(),glue_load()]

group_dag=task_group_dag()
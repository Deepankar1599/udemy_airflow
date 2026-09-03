from airflow.sdk import dag,task
import requests
import pandas as pd
import os
from sqlalchemy import create_engine, text
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag
def etl_pipeline():




    @task
    def timestamp():
        from datetime import datetime
        return datetime.now().isoformat() 

   

    
    @task
    def extract(ti):

        timestamp=ti.xcom_pull(task_ids="timestamp",key="return_value")
        url="http://fastapi:8000/fetch_data"
        response=requests.get(url)
        data=response.json().get("data",[])

        #creating staging directory if it does not exist

       
        os.makedirs("/tmp/raw",exist_ok=True)

        #writing data to staging layer

        with open(f"/tmp/raw/data_{timestamp}.csv","w") as f:
            f.write("id,name,age\n")
            for item in data:
                f.write(f"{item['id']},{item['name']},{item['age']}\n")

        return "Data extracted and stored in staging layer"


    @task
    def transform(ti):
        timestamp=ti.xcom_pull(task_ids="timestamp",key="return_value")

        #redaing data from staging layer
        df=pd.read_csv(f"/tmp/raw/data_{timestamp}.csv")

        df["age_group"]=df["age"].apply(lambda x:"Young" if x<30 else "Adult")

        #creating trransformed directory if it doesn't exist

        os.makedirs("/tmp/transformed",exist_ok=True)
        #Writing transformed data to transformed layer ("tmp/transformed/data_transformed")

        df.to_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv",index=False)

    @task
    def create_tables():
        query="""
        CREATE TABLE IF NOT EXISTS employees_new (
            id INT,
            name VARCHAR(255),
            age int,
            age_group VARCHAR(50)
            );
        """

        conn=create_engine("postgresql://airflow:airflow@postgres:5432/airflow").connect()
        
        with conn.begin() as transaction:
            try:
                conn.execute(text(query))
            except Exception as e:
                transaction.rollback()
                raise e
            else:
                transaction.commit()


    @task
    def load(ti):
        timestamp=ti.xcom_pull(task_ids="timestamp",key="return_value")

        df=pd.read_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv")

        #Loading data into postgresql
        engine=create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
        df.to_sql("employees_new",con=engine,if_exists="append",index=False)

        engine.dispose()

    #Task to create tables in postgress using sql operator

    create_new_table=SQLExecuteQueryOperator(
        task_id="create_student_table",
        conn_id="my_postgresql",
        sql="""
        CREATE TABLE IF NOT EXISTS STUDENTS (
                    id INT ,
                    name VARCHAR(255),
                    age int,
                    age_group VARCHAR(50));  
            """
    )
    #task to write the transfoemed data to a new table using the hooks

    @task
    def write_to_new_table(ti):
        timestamp=ti.xcom_pull(task_ids="timestamp",key="return_value")
        
        df=pd.read_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv")
        hook=PostgresHook(postgres_conn_id="my_postgresql")
        hook.copy_expert(
                sql="""
                COPY STUDENTS (id,name,age_grup)
                FROM STDIN WITH CSV HEADER
                """,
                filename=f"/tmp/transformed/data_transformed_{timestamp}.csv"
        )
    create_tables=create_tables()

    timestamp()>>extract()>>transform()>>[create_tables,create_new_table]
    create_tables>>load()
    create_new_table>>write_to_new_table()

etl_pipeline()
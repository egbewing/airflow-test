from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine, MetaData
import psycopg2
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, VARCHAR, Date, Enum, BigInteger, Table
import pendulum
from airflow.decorators import dag, task
import sys
sys.path.append('/home/ted/Documents/kc/blaze-retail-api')
from blaze_retail_api import blaze_retail_api


# Need
# file to handle connecting to db
# ^^ this file will also include upsert method

# DAG ARCHITECTURE
# PULL employees from blaze api (by modified date filter)
# UPSERT employees into table



@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 21),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
    )

def get_employees():
    @task(task_id='get_employee_records')
    def get_employee_records():
        blaze = blaze_retail_api(partner_key='', Authorization='')
        employees = blaze.get_employees()
        employees.to_csv('/home/ted/airflow/dags/files/employees.csv', index=False)
    
    @task(task_id='load_employee_records')
    def load_employees():
        pg_hook = PostgresHook(postgres_conn_id='blaze_db')
        engine = pg_hook.get_sqlalchemy_engine()
        base = declarative_base()
        meta = MetaData(engine)
        file_name = '/home/ted/airflow/dags/files/employees.csv'
        conn = engine.connect()

        class Employee(base):
            __tablename__ = 'dim_employees'

            emp_id = Column(Integer, primary_key=True)
            emp_first_nm = Column(VARCHAR(50))
            emp_last_nm = Column(VARCHAR(50))
            emp_email = Column(VARCHAR(50))

        with open(file_name, 'r') as input_file:
            emp = pd.read_csv(input_file)
        emp = emp[['firstName', 'lastName', 'email']]
        emp.columns = ['emp_first_nm', 'emp_last_nm', 'emp_email']
        rec_to_write = emp.to_dict(orient='records')
        table = Table('dim_employees', meta, autoload=True)

        Session = sessionmaker(bind=engine)
        session = Session()

        conn.execute(table.insert(), rec_to_write)
        session.commit()
        session.close()

    get_employee_records() >> load_employees()

dag = get_employees()
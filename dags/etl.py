import sql_statements

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from operators.load_dim import load_to_dim
from operators.load_fact import load_to_fact
from operators.load_staging import load_to_staging
from operators.load_serving import load_to_serving
from sqlalchemy import create_engine, inspect
from datetime import datetime, timedelta

#set default for DAG
default_args = {
    'owner' : 'zahra',
    'depend_on_past' : False,
    'start_date' : datetime(2023, 3, 25)}

#create DAG
with DAG(
    dag_id="simple_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:   
    #create connection
    engine = create_engine("postgresql+psycopg2://postgres:postgres@postgres:5432/postgres")
    engine.connect()

    #define source data path
    path_review = './data/yelp/yelp_academic_dataset_review.json'
    path_tip = './data/yelp/yelp_academic_dataset_tip.json'
    path_checkin = './data/yelp/yelp_academic_dataset_checkin.json'
    path_business = './data/yelp/yelp_academic_dataset_business.json'
    path_user = './data/yelp/yelp_academic_dataset_user.json'
    path_temperature = './data/temperature/USW00023169-temperature-degreeF.csv'
    path_precipitation = './data/temperature/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv'

    #define staging task
    load_stg_tip_task = PythonOperator(task_id="load_stg_tip_task", python_callable=load_to_staging(path_tip,'stg_tip',engine=engine).load_staging_tip())
    load_stg_checkin_task = PythonOperator(task_id="load_stg_checkin_task",python_callable=load_to_staging(path_checkin,'stg_checkin',engine=engine).load_staging_tip())
    load_stg_review_task = PythonOperator(task_id="load_stg_review_task", python_callable=load_to_staging(path_review,'stg_review',engine=engine).load_staging_review())
    load_stg_business_task = PythonOperator(task_id="load_stg_business_task", python_callable=load_to_staging(path_business,'stg_business',engine=engine).load_staging_business())
    load_stg_user_task = PythonOperator(task_id="load_stg_user_task",python_callable=load_to_staging(path_user,'stg_user',engine=engine).load_staging_user())
    load_stg_precipitation_task = PythonOperator(task_id="load_stg_precipitation_task", python_callable=load_to_staging(path_precipitation,'stg_precipitation',engine=engine).load_staging_precipitacion())
    load_stg_temperature_task = PythonOperator(task_id="load_stg_temperature_task",python_callable=load_to_staging(path_temperature,'stg_temperature',engine=engine).load_staging_temperature())
    
    #define fact task
    load_fact_review_task = PythonOperator(task_id="load_fact_review_task", python_callable=load_to_fact(sql_statements.create_table_fact_review,sql_statements.fact_review_insert, engine).load_to_fact())
    load_fact_tip_task = PythonOperator(task_id="load_fact_tip_task", python_callable=load_to_fact(sql_statements.create_table_fact_tip, sql_statements.fact_tip_insert, engine).load_to_fact())

    #define dim task
    load_dim_business_task = PythonOperator(task_id="load_dim_business_task", python_callable=load_to_dim(sql_statements.create_table_dim_business, sql_statements.dim_business_insert, engine).load_to_dim())
    load_dim_user_task = PythonOperator(task_id="run_tables_exists_task", python_callable=load_to_dim(sql_statements.create_table_dim_user, sql_statements.dim_user_insert, engine).load_to_dim())
    

    #define serving task
    load_AggReviewDay_task = PythonOperator(task_id="load_AggReviewDay_task", python_callable=load_to_serving(sql_statements.create_table_AggReviewDay, sql_statements.AggReviewDay_insert, engine).load_to_serving())
    load_AggTipDay_task = PythonOperator(task_id="load_AggTipDay_task", python_callable=load_to_serving(sql_statements.create_table_AggTipDay, sql_statements.AggTipDay_insert, engine).load_to_serving())

    #define task dependencies
    [load_stg_business_task, 
     load_stg_user_task, 
     load_stg_precipitation_task, 
     load_stg_temperature_task, 
     load_stg_tip_task, 
     load_stg_review_task] >> [load_fact_review_task, load_fact_tip_task] >> [load_dim_business_task, load_dim_user_task] >> [load_AggReviewDay_task, load_AggTipDay_task] 


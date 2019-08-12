"""
This DAG will do the following :
# copy data from AWS s3 to AWS redshift staging tables: stage_events and stage_songs
# load data from staging tables to star schema tables( fact table and dimension tables)
# data control check to see if data has been populated to star schema

# in case of failure, DAG would retry 3 times , after 5 minutes delay
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# these are set based on guideline
default_args = {
    'owner' : 'udacity',
    'depends_on_past' : False,
    'retries' : 3,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2019, 8,4 ),
    'email_on_retry' : False ,
    'catchup_by_default' : False    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@hourly'    
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)  


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/2018/11/2018-11-12-events.json",   
    json_path = "s3://udacity-dend/log_json_path.json",
    file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'staging_songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_data/A/B/C/TRABCEI128F424C983.json",
    json_path = "auto",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    redshift_conn_id = "redshift",
    load_sql_stmt = SqlQueries.songplay_table_insert 
)  

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = "redshift",
    load_sql_stmt = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    redshift_conn_id = "redshift",
    load_sql_stmt = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    redshift_conn_id = "redshift",
    load_sql_stmt = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'times',
    redshift_conn_id = "redshift",
    load_sql_stmt = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table = ['songplays', 'users', 'songs', 'artists', 'times'],
    redshift_conn_id = "redshift"    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# set up tasks dependancies
# use the varaibles not task_id
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_song_dimension_table >> run_quality_checks

load_songplays_table >> load_user_dimension_table
load_user_dimension_table >> run_quality_checks

load_songplays_table >> load_artist_dimension_table
load_artist_dimension_table >> run_quality_checks

load_songplays_table >> load_time_dimension_table
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator




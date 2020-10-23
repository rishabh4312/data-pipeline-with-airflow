from datetime import datetime, timedelta
import os
import sys
sys.path.insert(0,'.')
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_dimensions import LoadDimensionOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.operators.create_tables import CreateTableOperator

from plugins.helpers.sql_queries import SqlQueries
from plugins.helpers.create_tables_queries import createTable

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=300),
    'cathcup': False
}

dag = DAG('sparkify_analytical_tables_dag',
           default_args = default_args,
           description='Load and transform data in Redshift with Airflow',
        #    start_date = datetime.now() + timedelta(days=60),
           schedule_interval='0 * * * *'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Creating Tables:
create_staging_events_table_task = CreateTableOperator(
    task_id='create_staging_events_table_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_staging_events_table
)

create_table_staging_songs_task = CreateTableOperator(
    task_id='create_table_staging_songs_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_table_staging_songs
)

create_artist_table_task = CreateTableOperator(
    task_id='create_artist_table_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_artist_table
)

create_songplays_table_task = CreateTableOperator(
    task_id='create_songplays_table_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_songplays_table
)

create_songs_table_task = CreateTableOperator(
    task_id='create_songs_table_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_songs_table
)

create_table_time_task = CreateTableOperator(
    task_id='create_table_time_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_table_time
)

create_table_users_task = CreateTableOperator(
    task_id='create_table_users_task',
    redshift_conn_id = "redshift",
    dag=dag,
    sql = createTable.create_table_users
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="bucket-by-rishabh",
    s3_key="log_data",
    json_path="s3://bucket-by-rishabh/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,

    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="bucket-by-rishabh",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    select_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    truncate_table=True,
    select_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    truncate_table=True,
    select_query=SqlQueries.songtable_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    truncate_table=True,
    select_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    truncate_table=True,
    select_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Create Tables
start_operator >> create_staging_events_table_task
create_staging_events_table_task >> create_table_staging_songs_task
create_table_staging_songs_task >> create_artist_table_task
create_artist_table_task >> create_songplays_table_task
create_songplays_table_task >> create_songs_table_task
create_songs_table_task >> create_table_time_task
create_table_time_task >> create_table_users_task


# Step 1
create_table_users_task >> stage_events_to_redshift
create_table_users_task >> stage_songs_to_redshift

# Step 2
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Step 3
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Step 4
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Step 5 - end
run_quality_checks >> end_operator

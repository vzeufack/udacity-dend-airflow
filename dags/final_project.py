from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='start_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        s3_key="log-data/",
        json_format="log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        s3_key="song-data/"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        target_table="songplays",
        sql_statement=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        target_table="users",
        sql_statement=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        target_table="songs",
        sql_statement=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        target_table="artists",
        sql_statement=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        target_table="time",
        sql_statement=SqlQueries.time_table_insert
    )

    dq_checks = [
        {
            "sql": "SELECT COUNT(*) FROM staging_events",
            "expected_result": 1,
            "comparison": ">",
            "description": "staging_events table should not be empty"
        },
        {
            "sql": "SELECT COUNT(*) FROM staging_songs",
            "expected_result": 1,
            "comparison": ">",
            "description": "staging_songs table should not be empty"
        },
        {
            "sql": "SELECT COUNT(*) FROM songplays",
            "expected_result": 1,
            "comparison": ">",
            "description": "songplays table should not be empty"
        },
        {
            "sql": "SELECT COUNT(*) FROM songs",
            "expected_result": 1,
            "comparison": ">",
            "description": "songs table should not be empty"
        },
        {
            "sql": "SELECT COUNT(*) FROM artists",
            "expected_result": 1,
            "comparison": ">",
            "description": "artists table should not be empty"
        },
        {
            "sql": "SELECT COUNT(*) FROM users",
            "expected_result": 1,
            "comparison": ">",
            "description": "users table should not be empty"
        },
        {
            "sql": "SELECT COUNT(*) FROM time",
            "expected_result": 1,
            "comparison": ">",
            "description": "time table should not be empty"
        }
    ]

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        checks=dq_checks
    )

    end_operator = DummyOperator(task_id='end_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()

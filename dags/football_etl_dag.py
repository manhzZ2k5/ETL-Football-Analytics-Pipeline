from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent.absolute()
SCR_DIR = BASE_DIR / "scr"
sys.path.insert(0, str(SCR_DIR))

os.environ["ETL_FOOTBALL_BASE_DIR"] = str(BASE_DIR)

default_args = {
    'owner': 'football_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'football_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for Football data: Extract from FBref -> Transform -> Load to PostgreSQL',
    schedule_interval='0 2 * * 3',  # Every Wednesday at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'football', 'postgresql', 'production'],
)


def extract_task(**context):
    try:
        print("Fetching data from FBref...")
        try:
            from Extract_Fixed import main as extract_main
            print("[INFO] Using Extract_Fixed.py with FBref bypass")
        except ImportError:
            from Extract import main as extract_main
            print("[WARN] Extract_Fixed not found, using standard Extract")
        
        extract_main()
        
        print("[SUCCESS] Extract completed")
        
    except Exception as e:
        print(f"[ERROR] Extract failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def transform_task(**context):
    try:
        import Transform
        
        print("[INFO] Creating dimension tables...")
        Transform.create_dim_player()
        Transform.create_dim_team()
        Transform.create_dim_stadium()
        Transform.create_dim_match()
        
        if hasattr(Transform, 'create_dim_season'):
            Transform.create_dim_season()
        
        Transform.create_fact_team_match()
        Transform.create_fact_player_match()
        Transform.create_fact_team_point()
        
        print("Transform completed")
        
    except Exception as e:
        print(f"Transform failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def load_task(**context):
    try:
        from Load import (
            load_config,
            connect,
            load_dim_stadium,
            load_dim_team,
            load_dim_match,
            load_dim_player,
            load_dim_season,
            fact_team_match,
            load_fact_team_point,
            fact_player_match
        )
        
        DATA_PROCESSED_DIR = BASE_DIR / "data_processed"
        
        print("Connecting to PostgreSQL")
        original_cwd = os.getcwd()
        try:
            os.chdir(SCR_DIR)
            config = load_config()
        finally:
            os.chdir(original_cwd)
        
        conn = connect(config)
        if conn is None:
            raise Exception("Failed connect")
        
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            print("Loading Dim Tables")
            load_dim_stadium(cursor, str(DATA_PROCESSED_DIR / 'dim_stadium.csv'))
            load_dim_team(cursor, str(DATA_PROCESSED_DIR / 'dim_team.csv'))
            load_dim_match(cursor, str(DATA_PROCESSED_DIR / 'dim_match.csv'))
            load_dim_player(cursor, str(DATA_PROCESSED_DIR / 'dim_player.csv'))
            load_dim_season(cursor, str(DATA_PROCESSED_DIR / 'dim_season.csv'))
            
            print("Loading Fact Tables...")
            fact_team_match(cursor, str(DATA_PROCESSED_DIR / 'fact_team_match_clean.csv'))
            load_fact_team_point(cursor, str(DATA_PROCESSED_DIR / 'fact_team_point.csv'))
            fact_player_match(cursor, str(DATA_PROCESSED_DIR / 'fact_player_match_clean.csv'))
            
            print(" Load completed")
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Load failed")
        import traceback
        traceback.print_exc()
        raise


start = EmptyOperator(
    task_id='start',
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    dag=dag,
)


start >> extract >> transform >> load

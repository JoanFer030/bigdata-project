"""
Bronze DAG for MITMA data ingestion.
Orchestrates all Bronze layer tasks using Airflow's @task decorator pattern.
"""

from datetime import datetime
from airflow.sdk import dag
from airflow.models.param import Param

# Import infrastructure setup tasks
from bronze.tasks.setup_infrastructure import verify_connections, ensure_rustfs_bucket

# Import data ingestion tasks
from bronze.tasks.mitma_od import load_od_matrices
from bronze.tasks.mitma_people_day import load_people_day
from bronze.tasks.mitma_overnights import load_overnight_stay
from bronze.tasks.mitma_zonification import load_zonification_data


@dag(
    dag_id="bronze_mitma_all_datasets",
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['bronze', 'mitma', 'data-ingestion'],
    params={
        "start": Param(
            type="string",
            description="Start date for data loading (YYYY-MM-DD)"
        ),
        "end": Param(
            type="string",
            description="End date for data loading (YYYY-MM-DD)"
        ),
    },
    description="Complete Bronze layer pipeline for MITMA data (OD, people_day, overnight_stay, zonification)"
)

def bronze_mitma_pipeline():
    """
    Bronze layer DAG that loads all MITMA datasets.
    
    This DAG orchestrates multiple tasks to load:
    - OD matrices (viajes)
    - People day (personas por día)
    - Overnight stay (pernoctaciones)
    - Zonification (geometries and metadata)
    
    For multiple zone types: distritos, municipios, GAU
    """
    
    # =======================================================
    # STEP 1: Infrastructure Setup (runs first)
    # =======================================================
    # Verify that PostgreSQL and RustFS are accessible
    verify_task = verify_connections()
    
    # Ensure bucket exists (creates if needed)
    bucket_task = ensure_rustfs_bucket()
    
    # Setup tasks run sequentially: verify -> create bucket
    verify_task >> bucket_task
    
    # =======================================================
    # STEP 2: Data Ingestion (runs after setup)
    # =======================================================
    # Define zone types to process
    zone_types = ['distritos', 'municipios', 'gau']
    
    # Create tasks for each zone type
    for zone_type in zone_types:
        # Time series tasks
        od_task = load_od_matrices.override(task_id=f"load_od_{zone_type}")(
            zone_type=zone_type,
            start_date='{{ params.start }}',
            end_date='{{ params.end }}'
        )
        
        people_task = load_people_day.override(task_id=f"load_people_day_{zone_type}")(
            zone_type=zone_type,
            start_date='{{ params.start }}',
            end_date='{{ params.end }}'
        )
        
        overnight_task = load_overnight_stay.override(task_id=f"load_overnight_stay_{zone_type}")(
            zone_type=zone_type,
            start_date='{{ params.start }}',
            end_date='{{ params.end }}'
        )
        
        # Zonification task
        zonif_task = load_zonification_data.override(task_id=f"load_zonification_{zone_type}")(
            zone_type=zone_type
        )
        
        # Setup must complete before data ingestion starts
        bucket_task >> [od_task, people_task, overnight_task, zonif_task]


# Instantiate the DAG
dag_instance = bronze_mitma_pipeline()

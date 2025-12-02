"""
Main DAG for Data Ingestion Pipeline.
Orchestrates all layers (currently Bronze) and infrastructure setup.
"""

from datetime import datetime
from airflow.sdk import dag
from airflow.models.param import Param

# Import infrastructure setup tasks (Global)
from tasks.verify_connections import PRE_verify_connections
from tasks.ensure_rustfs_bucket import PRE_ensure_rustfs_bucket

# Import Bronze layer tasks
from bronze.tasks.mitma.mitma_od import BRONZE_mitma_od
from bronze.tasks.mitma.mitma_people_day import BRONZE_mitma_people_day
from bronze.tasks.mitma.mitma_overnights import BRONZE_mitma_overnight_stay
from bronze.tasks.mitma.mitma_zonification import BRONZE_mitma_zonification
from bronze.tasks.mitma_ine_relations import BRONZE_mitma_ine_relations
from bronze.tasks.ine.ine_municipios import BRONZE_ine_municipios
from bronze.tasks.ine.ine_empresas import BRONZE_ine_empresas_municipio
from bronze.tasks.ine.ine_poblacion import BRONZE_ine_poblacion_municipio
from bronze.tasks.ine.ine_renta import BRONZE_ine_renta_municipio


@dag(
    dag_id="main_data_pipeline",
    start_date=datetime(2025, 12, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['bronze', 'mitma', 'ine', 'data-ingestion', 'main'],
    params={
        "start": Param(
            type="string",
            description="Start date for MITMA data loading (YYYY-MM-DD)"
        ),
        "end": Param(
            type="string",
            description="End date for MITMA data loading (YYYY-MM-DD)"
        ),
    },
    description="Complete Data Pipeline (Infrastructure + Bronze Layer)"
)
def main_pipeline():
    """
    Main DAG that orchestrates the entire data pipeline.
    
    Currently includes:
    1. Infrastructure Setup (Postgres, RustFS)
    2. Bronze Layer Ingestion:
       - MITMA: OD matrices, People day, Overnight stay, Zonification
       - INE: Municipios, Empresas, Poblacion, Renta
    """
    
    # =======================================================
    # STEP 1: Infrastructure Setup (runs first)
    # =======================================================
    # Verify that PostgreSQL and RustFS are accessible
    verify_task = PRE_verify_connections()
    
    # Ensure bucket exists (creates if needed)
    bucket_task = PRE_ensure_rustfs_bucket()
    
    # Setup tasks run sequentially: verify -> create bucket
    verify_task >> bucket_task
    
    # =======================================================
    # STEP 2: MITMA Data Ingestion
    # =======================================================
    # Define zone types to process
    zone_types = ['distritos', 'municipios', 'gau']
    
    mitma_tasks = []
    
    # Create tasks for each zone type
    for zone_type in zone_types:
        # Time series tasks
        od_task = BRONZE_mitma_od.override(task_id=f"BRONZE_mitma_od_{zone_type}")(
            zone_type=zone_type,
            start_date='{{ params.start }}',
            end_date='{{ params.end }}'
        )
        
        people_task = BRONZE_mitma_people_day.override(task_id=f"BRONZE_mitma_people_day_{zone_type}")(
            zone_type=zone_type,
            start_date='{{ params.start }}',
            end_date='{{ params.end }}'
        )
        
        overnight_task = BRONZE_mitma_overnight_stay.override(task_id=f"BRONZE_mitma_overnight_stay_{zone_type}")(
            zone_type=zone_type,
            start_date='{{ params.start }}',
            end_date='{{ params.end }}'
        )
        
        # Zonification task
        zonif_task = BRONZE_mitma_zonification.override(task_id=f"BRONZE_mitma_zonification_{zone_type}")(
            zone_type=zone_type
        )
        
        mitma_tasks.extend([od_task, people_task, overnight_task, zonif_task])

    # Relations task (doesn't depend on zone_type loop)
    relations_task = BRONZE_mitma_ine_relations()
    mitma_tasks.append(relations_task)

    # =======================================================
    # STEP 3: INE Data Ingestion
    # =======================================================
    
    ine_municipios_task = BRONZE_ine_municipios()
    
    # Derive year from start date (YYYY-MM-DD) -> YYYY
    ine_year = '{{ params.start[:4] }}'
    
    ine_empresas_task = BRONZE_ine_empresas_municipio(
        year=ine_year
    )
    
    ine_poblacion_task = BRONZE_ine_poblacion_municipio(
        year=ine_year
    )
    
    ine_renta_task = BRONZE_ine_renta_municipio(
        year=ine_year
    )
    
    ine_tasks = [ine_municipios_task, ine_empresas_task, ine_poblacion_task, ine_renta_task]
        
    # Setup must complete before any data ingestion starts
    bucket_task >> mitma_tasks
    bucket_task >> ine_tasks


# Instantiate the DAG
dag_instance = main_pipeline()

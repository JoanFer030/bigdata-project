"""
Airflow task for loading INE Municipios data into Bronze layer.
"""

import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import create_and_merge_table_from_json, get_ducklake_connection


@task
def BRONZE_ine_municipios():
    """
    Load municipios from INE datasource.
    """
    table_name = 'ine_municipios'
    url = 'https://servicios.ine.es/wstempus/js/ES/VALORES_VARIABLE/19'
    
    print(f"[TASK] Starting INE Municipios load from {url}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Use 'Id' as the primary key for municipios
    row_count = create_and_merge_table_from_json(
        con,
        table_name, 
        url,
        key_columns=['Id']  # Assuming 'Id' is the unique identifier
    )
    
    msg = f"Successfully loaded INE Municipios: {row_count:,} records"
    print(f"[TASK] {msg}")
    
    full_table_name = f'bronze_{table_name}'
    print(f"[TASK] Sample data from {full_table_name}:")
    print(con.execute(f"SELECT * FROM {full_table_name} LIMIT 10").fetchdf())
    
    return {
        'status': 'success',
        'message': msg,
        'records': row_count,
        'table_name': f'bronze_{table_name}'
    }

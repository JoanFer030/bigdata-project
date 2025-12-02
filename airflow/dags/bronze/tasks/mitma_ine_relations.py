"""
Airflow task for loading MITMA-INE relations data into Bronze layer.
"""

import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import create_and_merge_table, get_ducklake_connection


@task
def BRONZE_mitma_ine_relations():
    """
    Load MITMA-INE relations table (cross-reference between zonifications).
    """
    table_name = 'mitma_ine_relations'
    url = "https://movilidad-opendata.mitma.es/zonificacion/relacion_ine_zonificacionMitma.csv"
    
    print(f"[TASK] Starting MITMA-INE relations load from {url}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Create and merge table
    # Note: create_and_merge_table expects a list of URLs
    create_and_merge_table(con, None, table_name, [url])
    
    # Get count for verification
    full_table_name = f'bronze_{table_name}'
    count = con.execute(f"SELECT COUNT(*) as count FROM {full_table_name}").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    msg = f"Successfully loaded MITMA-INE relations: {record_count:,} records"
    print(f"[TASK] {msg}")
    print(f"[TASK] Sample data from {full_table_name}:")
    print(con.execute(f"SELECT * FROM {full_table_name} LIMIT 10").fetchdf())
    
    return {
        'status': 'success',
        'message': msg,
        'records': record_count,
        'table_name': full_table_name
    }

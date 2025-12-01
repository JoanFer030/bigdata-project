"""
Airflow task for loading MITMA zonification data into Bronze layer.
Handles zoning (zonificación) data including geometries, names, and population
for distritos, municipios, and GAU zone types.
"""

import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import load_zonificacion, get_ducklake_connection


@task
def load_zonification_data(zone_type: str = 'distritos'):
    """
    Airflow task to load zonification data into DuckDB for the specified type.
    
    This function downloads shapefiles, CSVs with names and population,
    merges them, and loads into a bronze layer table.
    
    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau' (default: 'distritos')
    
    Returns:
    - Dict with task status and info
    """
    print(f"[TASK] Starting zonification load for {zone_type}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Load zonification data using utility function
    load_zonificacion(con, zone_type)
    
    # Get count for verification
    table_name = f'bronze_mitma_{zone_type}'
    count = con.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    msg = f"Successfully loaded zonification data for {zone_type}: {record_count:,} records"
    print(f"[TASK] {msg}")
    
    return {
        'status': 'success',
        'message': msg,
        'zone_type': zone_type,
        'dataset': 'zonification',
        'records': record_count,
        'table_name': table_name
    }

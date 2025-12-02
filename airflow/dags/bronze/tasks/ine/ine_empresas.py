"""
Airflow task for loading INE Empresas per Municipio data into Bronze layer.
"""

import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import create_and_merge_table_from_json, get_ducklake_connection


@task
def BRONZE_ine_empresas_municipio(year: int = 2023):
    """
    Load empresas from INE datasource.
    
    Parameters:
    - year: Year to fetch data for (default: 2023)
    """
    table_name = 'ine_empresas_municipio'
    url = f'https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/4721?date={year}0101:{year}1231&Tv=40621:248341&Tv=selCri_2:on'
    
    print(f"[TASK] Starting INE Empresas load for year {year} from {url}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Use 'COD' as the primary key (based on notebook usage, though notebook comment said 'Id', code used 'COD')
    # In notebook: create_and_merge_table_from_json(table_name, url, ['COD'])
    row_count = create_and_merge_table_from_json(
        con,
        table_name, 
        url,
        key_columns=['COD']
    )
    
    msg = f"Successfully loaded INE Empresas for year {year}: {row_count:,} records"
    print(f"[TASK] {msg}")

    full_table_name = f'bronze_{table_name}'
    print(f"[TASK] Sample data from {full_table_name}:")
    print(con.execute(f"SELECT * FROM {full_table_name} LIMIT 10").fetchdf())
    
    return {
        'status': 'success',
        'message': msg,
        'year': year,
        'records': row_count,
        'table_name': f'bronze_{table_name}'
    }

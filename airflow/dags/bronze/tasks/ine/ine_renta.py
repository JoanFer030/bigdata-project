"""
Airflow task for loading INE Renta per Municipio data into Bronze layer.
"""

import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import create_and_merge_table_from_json, get_ducklake_connection


@task
def get_ine_renta_urls(year: int = 2023):
    """
    Generate the list of URLs for INE Renta data.
    """
    # All table IDs for renta media and mediana indicators
    # Source: https://servicios.ine.es/wstempus/js/ES/TABLAS_OPERACION/353
    base_ids = [
        30656, 30833, 30842, 30851, 30860, 30869, 30878, 30887, 30896,
        30917, 30926, 30935, 30944, 30953, 30962, 30971, 30980, 30989, 30998,
        31007, 31016, 31025, 31034, 31043, 31052, 31061, 31070, 31079, 31088,
        31097, 31106, 31115, 31124, 31133, 31142, 31151, 31160, 31169, 31178,
        31187, 31196, 31205, 31214, 31223, 31232, 31241, 31250, 31259, 31268,
        31277, 31286, 31295
    ]
    
    # Build URLs for all table IDs
    urls = [f'https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{id}?date={year}0101' for id in base_ids]
    return urls

@task
def BRONZE_ine_renta_municipio(url: str, year: int = 2023):
    """
    Load renta from INE datasource for a specific URL.
    Designed to be mapped over a list of URLs for concurrent execution.
    
    Parameters:
    - url: The specific INE table URL to load
    - year: Year (for metadata/logging)
    """
    table_name = 'ine_renta_municipio'
    
    print(f"[TASK] Starting INE Renta load for year {year} from {url}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Use 'COD' as the primary key
    row_count = create_and_merge_table_from_json(
        con,
        table_name, 
        url,
        key_columns=['COD']
    )
    
    msg = f"Successfully loaded INE Renta data: {row_count:,} records"
    print(f"[TASK] {msg}")
    
    return {
        'status': 'success',
        'message': msg,
        'year': year,
        'records': row_count,
        'table_name': f'bronze_{table_name}',
        'url': url
    }


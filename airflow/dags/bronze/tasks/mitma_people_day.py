"""
Airflow task for loading MITMA people_day (personas por día) data into Bronze layer.
Handles daily people movement data for distritos, municipios, and GAU zone types.
"""

import sys
import os
from airflow.sdk import task

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_mitma_urls, create_and_merge_table, get_ducklake_connection


@task
def load_people_day(zone_type: str = 'distritos', start_date: str = None, end_date: str = None):
    """
    Airflow task to load people_day data for a specific type and date range.
    
    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau' (default: 'distritos')
    - start_date: Start date in 'YYYY-MM-DD' format (default: '2022-03-01')
    - end_date: End date in 'YYYY-MM-DD' format (default: '2022-03-03')
    
    Returns:
    - Dict with task status and info
    """
    dataset = 'people_day'
    
    print(f"[TASK] Starting people_day load for {zone_type} from {start_date} to {end_date}")
    
    # Get connection (singleton - will be reused)
    con = get_ducklake_connection()
    
    # Get URLs from RSS feed
    urls = get_mitma_urls(dataset, zone_type, start_date, end_date)
    
    if not urls:
        msg = f"No URLs found for {dataset} {zone_type} between {start_date} and {end_date}"
        print(f"[TASK] {msg}")
        return {
            'status': 'no_data',
            'message': msg,
            'zone_type': zone_type,
            'dataset': dataset
        }
    
    # Create and merge table with data
    create_and_merge_table(con, dataset, zone_type, urls)
    
    # Get count for verification
    table_name = f'bronze_mitma_{dataset}_{zone_type}'
    count = con.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchdf()
    record_count = int(count['count'].iloc[0])
    
    msg = f"Successfully loaded people_day data for {zone_type}: {record_count:,} records"
    print(f"[TASK] {msg}")
    
    return {
        'status': 'success',
        'message': msg,
        'zone_type': zone_type,
        'dataset': dataset,
        'urls_processed': len(urls),
        'records': record_count,
        'table_name': table_name
    }

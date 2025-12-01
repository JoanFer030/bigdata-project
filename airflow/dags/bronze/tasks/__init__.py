"""
Bronze Layer Airflow Tasks Package
Contains individual Airflow task modules for MITMA data ingestion.
All tasks use the @task decorator from airflow.sdk
"""

from .mitma_od import load_od_matrices
from .mitma_people_day import load_people_day
from .mitma_overnights import load_overnight_stay
from .mitma_zonification import load_zonification_data

__all__ = [
    'load_od_matrices',
    'load_people_day',
    'load_overnight_stay',
    'load_zonification_data'
]

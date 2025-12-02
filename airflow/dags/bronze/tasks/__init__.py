"""
Bronze Layer Airflow Tasks Package
Contains individual Airflow task modules for MITMA and INE data ingestion.
All tasks use the @task decorator from airflow.sdk
"""

from .mitma.mitma_od import BRONZE_mitma_od
from .mitma.mitma_people_day import BRONZE_mitma_people_day
from .mitma.mitma_overnights import BRONZE_mitma_overnight_stay
from .mitma.mitma_zonification import BRONZE_mitma_zonification
from .mitma_ine_relations import BRONZE_mitma_ine_relations

from .ine.ine_municipios import BRONZE_ine_municipios
from .ine.ine_empresas import BRONZE_ine_empresas_municipio
from .ine.ine_poblacion import BRONZE_ine_poblacion_municipio
from .ine.ine_renta import BRONZE_ine_renta_municipio

__all__ = [
    'BRONZE_mitma_od',
    'BRONZE_mitma_people_day',
    'BRONZE_mitma_overnight_stay',
    'BRONZE_mitma_zonification',
    'BRONZE_mitma_ine_relations',
    'BRONZE_ine_municipios',
    'BRONZE_ine_empresas_municipio',
    'BRONZE_ine_poblacion_municipio',
    'BRONZE_ine_renta_municipio'
]

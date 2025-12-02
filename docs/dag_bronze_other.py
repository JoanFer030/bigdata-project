import os
from datetime import datetime
from airflow.sdk import  dag, task # type: ignore


from utils import connect_datalake 

@task
def ingest_holidays():
    con = connect_datalake()

    try:
        con.execute("""
            INSTALL httpfs;
            LOAD httpfs;
        """)

        con.execute("""
            CREATE OR REPLACE TABLE spanish_holidays AS
            SELECT 
                startDate AS fecha,
                name[1].text AS nombre
            FROM read_json(
                'https://openholidaysapi.org/PublicHolidays?countryIsoCode=ES&languageIsoCode=ES&validFrom=2023-01-01&validTo=2023-12-31',
                format='array'
            )
            WHERE nationwide = true;
        """)
    except Exception as e:
        print(f"Error loading holidays data: {e}")
        raise e
    finally:
        con.close()


@task
def ingest_relacion_ine_mitma(data_path: str):
    con = connect_datalake()

    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze_relacion_ine_mitma (
                seccion_ine             VARCHAR,
                distrito_ine            VARCHAR,
                municipio_ine           VARCHAR,
                distrito_mitma          VARCHAR,
                municipio_mitma         VARCHAR,
                gau_mitma               VARCHAR,
                loaded_at  TIMESTAMP,
                source_file TEXT
            );
        """)

        con.execute(f"""
            INSERT INTO bronze_relacion_ine_mitma (
                seccion_ine, distrito_ine, municipio_ine, distrito_mitma, municipio_mitma, gau_mitma, loaded_at, source_file
            )
            SELECT
                seccion_ine,
                distrito_ine,
                municipio_ine,
                distrito_mitma,
                municipio_mitma,
                gau_mitma,
                CURRENT_TIMESTAMP AS loaded_at,
                filename AS source_file
            FROM read_csv(
                '{data_path}',
                filename = true,
                all_varchar = true,
                delim = '|'
            );
        """)

        print(f"Successfully loaded: {data_path}")
    except Exception as e:
        print(f"Error loading relacion_ine_mitma data: {e}")
        raise e
    finally:
        con.close()


@dag(
    dag_id="other_sources_ingest",
    start_date=datetime(2025, 11, 27),
    schedule=None,  
    catchup=False,
)
def other_data_ingest_dag(data_path):
    ingest_holidays()
    ingest_relacion_ine_mitma(data_path)


other_data_ingest_dag("include/resources/relacion_ine_mitma.csv")
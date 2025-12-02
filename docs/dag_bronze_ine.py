import os
from datetime import datetime
from airflow.sdk import  dag, task # type: ignore


from utils import connect_datalake


@task
def fetch_ine_data_paths(base_path: str):
    file_names = os.listdir(base_path)

    ine_data = []
    for file_name in file_names:
        path = os.path.join(base_path, file_name)
        table_name = f"bronze_{file_name.split('.')[0]}"

        ine_data.append({
            "path": path,
            "table_name": table_name
        })
    return ine_data

@task
def create_tables_ine():
    con = connect_datalake()

    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze_poblacion_municipios (
                municipio       VARCHAR,
                sexo            VARCHAR,
                edad            VARCHAR,
                periodo         VARCHAR,
                total           VARCHAR,
                loaded_at  TIMESTAMP,
                source_file TEXT
            );
        """)
        con.execute(f"""
            CREATE TABLE  IF NOT EXISTS bronze_empresas_municipios (
                municipio      VARCHAR,
                anyo           VARCHAR,
                total          VARCHAR,
                loaded_at TIMESTAMP,
                source_file TEXT
            );
        """)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS bronze_renta_distritos (
                municipio      VARCHAR,
                distrito       VARCHAR,
                seccion        VARCHAR,
                indicador      VARCHAR,
                anyo           VARCHAR,
                total          VARCHAR,
                loaded_at TIMESTAMP,
                source_file TEXT
            );
        """)
    except Exception as e:
        print(f"Error creating the tables: {e}")
        raise e
    finally:
        con.close()

@task(
    max_active_tis_per_dag=1
)
def ingest_data_ine(data_info):
    path = data_info["path"]
    table_name = data_info["table_name"]

    # Base de la creación de tabla
    query = f"""
        INSERT INTO {table_name} (
    """

    # Extensiones según la tabla objetivo
    if table_name == "bronze_poblacion_municipios":
        query += """
                sexo, municipio, edad, periodo, total, loaded_at, source_file
            )
            SELECT
                Sexo,
                Municipios,
                Edad,
                Periodo,
                Total,
        """

    elif table_name == "bronze_empresas_municipios":
        query += """
                municipio, anyo, total, loaded_at, source_file
            )
            SELECT
                Municipios,
                Periodo,
                Total,
        """

    elif table_name == "bronze_renta_distritos":
        query += """
                municipio, distrito, seccion, indicador, anyo, total, loaded_at, source_file
            )
            SELECT
                Municipios,
                Distritos,
                Secciones,
                Indicador,
                Periodo,
                Total,
        """
    else:
        raise ValueError("table_name does not exist")

    # Parte final común
    query += f"""
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            '{path}',
            filename = true,
            all_varchar = true
        );
    """

    con = connect_datalake()

    try:
        con.execute(query)
    except Exception as e:
        print(f"Error loading {table_name} table: {e}")
        raise e
    finally:
        con.close()


    
@dag(
    dag_id="ine_ingest",
    start_date=datetime(2025, 11, 27),
    schedule=None,  
    catchup=False,
)
def ine_data_ingest_dag(base_path):
    create_tables_ine()

    paths = fetch_ine_data_paths(base_path)

    ingest_data_ine.expand(
        data_info = paths
    )


ine_data_ingest_dag("include/resources/INE/")
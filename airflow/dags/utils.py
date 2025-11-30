import os
import duckdb

def connect_datalake():
    PROJECT_PATH = "include/project-bigdata"
    METADATA_PATH = os.path.join(PROJECT_PATH, "metadata.db")
    DATA_PATH = os.path.join(PROJECT_PATH, "ducklake_data")

    # Crear directorios si no existen
    os.makedirs(DATA_PATH, exist_ok=True)
    
    con = duckdb.connect()

    # Instalamos para crear el datalake
    con.execute("""
        INSTALL ducklake;
        LOAD ducklake;
    """)

    ##############################################
    ###                DATALAKE                ###
    ##############################################
    con.execute(f"""
    CREATE OR REPLACE SECRET secret_local_ducklake (
        TYPE ducklake,
        METADATA_PATH '{METADATA_PATH}',
        METADATA_PARAMETERS MAP {{'TYPE': 'duckdb'}}
    );
    """)

    con.execute(f"""
    ATTACH 'ducklake:secret_local_ducklake' AS project_db (DATA_PATH '{DATA_PATH}')
    """)

    con.execute("USE project_db")

    return con

# def close_datalake():

# # Crear tabla desde archivo local
# con.execute("""
# CREATE OR REPLACE TABLE viajes_distritos AS (
#     SELECT * FROM './resources/20230101_Viajes_distritos_v2.csv.gz'
# )
# """)

# # Ver los datos
# resultado = con.execute("""
# SELECT * FROM viajes_distritos LIMIT 100;
# """).fetchdf()

# print(resultado)
# con.close()
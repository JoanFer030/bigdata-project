import os
import duckdb
from dotenv import load_dotenv

def connect_datalake():
    PROJECT_PATH = "include/datalake"
    METADATA_PATH = os.path.join(PROJECT_PATH, "metadata.db")
    DATA_PATH = os.path.join(PROJECT_PATH, "data")

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

def connect_datalake_rustfs():
    load_dotenv('include/.env', override=True) 
    RUSTFS_HOST = os.getenv('RUSTFS_HOST', 'localhost')
    RUSTFS_PORT = os.getenv('RUSTFS_PORT', '8080')
    RUSTFS_USER = os.getenv('RUSTFS_USER', 'admin')
    RUSTFS_PASSWORD = os.getenv('RUSTFS_PASSWORD', 'password')
    RUSTFS_BUCKET = os.getenv('RUSTFS_BUCKET', 'mitma')
    RUSTFS_SSL = os.getenv('RUSTFS_SSL', 'false')

    # Postgres Configuration
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'muceim')

    # Construct S3 Endpoint with protocol
    S3_ENDPOINT = f"{RUSTFS_HOST}:{RUSTFS_PORT}"

    con = duckdb.connect()

    # Instalamos para crear el datalake
    con.execute("""
        INSTALL ducklake;
        LOAD ducklake;
        INSTALL postgres;
        LOAD postgres;
        INSTALL httpfs;
        LOAD httpfs;
    """)

    ##############################################
    ###                DATALAKE                ###
    ##############################################
    # Configure S3 Secrets for RustFS
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
    con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
    con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
    con.execute("SET s3_url_style='path';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET max_temp_directory_size='40GiB';")

    # Attach DuckLake with Postgres Catalog
    postgres_connection_string = f"dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}"
    attach_query = f"""
        ATTACH 'ducklake:postgres:{postgres_connection_string}' AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
    """
    con.execute(attach_query)

    con.execute("USE ducklake;")

    return con
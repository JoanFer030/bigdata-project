import os
import duckdb
from dotenv import load_dotenv # type: ignore


def connect_datalake():
    """
    Conexión LOCAL a DuckLake (solo para desarrollo local)
    Esta función crea un datalake en el sistema de archivos local.
    NO usa PostgreSQL ni RustFS.
    """
    PROJECT_PATH = "include/datalake"
    METADATA_PATH = os.path.join(PROJECT_PATH, "metadata.db")
    DATA_PATH = os.path.join(PROJECT_PATH, "data")

    # Crear directorios si no existen
    os.makedirs(DATA_PATH, exist_ok=True)
    
    con = duckdb.connect()

    # Instalamos extensiones necesarias
    con.execute("""
        INSTALL ducklake;
        LOAD ducklake;
    """)

    # Crear secret para DuckLake local
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
    """
    Conexión a DuckLake usando PostgreSQL + RustFS
    Esta es la función correcta para usar en producción con Airflow.
    
    IMPORTANTE: Esta función lee las variables de entorno desde .env
    En Airflow, deberías usar las conexiones de Airflow en lugar de .env
    """
    load_dotenv('include/.env', override=True) 
    RUSTFS_HOST = os.getenv('RUSTFS_HOST', 'localhost')
    RUSTFS_PORT = os.getenv('RUSTFS_PORT', '9000')
    RUSTFS_USER = os.getenv('RUSTFS_USER', 'admin')
    RUSTFS_PASSWORD = os.getenv('RUSTFS_PASSWORD', 'password')
    RUSTFS_BUCKET = os.getenv('RUSTFS_BUCKET', 'mitma')
    RUSTFS_SSL = os.getenv('RUSTFS_SSL', 'false')

    # Postgres Configuration
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'mitma')

    # Construct S3 Endpoint
    S3_ENDPOINT = f"{RUSTFS_HOST}:{RUSTFS_PORT}"

    con = duckdb.connect()

    # Instalar y cargar extensiones necesarias
    con.execute("""
        INSTALL ducklake;
        LOAD ducklake;
        INSTALL postgres;
        LOAD postgres;
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL spatial;
        LOAD spatial;
    """)

    # Configurar credenciales S3 para RustFS
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
    con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
    con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
    con.execute("SET s3_url_style='path';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET max_temp_directory_size='40GiB';")

    # Attach DuckLake con catálogo PostgreSQL y datos en RustFS
    postgres_connection_string = f"dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}"
    attach_query = f"""
        ATTACH 'ducklake:postgres:{postgres_connection_string}' AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
    """
    con.execute(attach_query)

    con.execute("USE ducklake;")

    return con


def connect_datalake_from_airflow():
    """
    Conexión a DuckLake usando las conexiones configuradas en Airflow
    Esta es la función RECOMENDADA para usar en tus DAGs de Airflow
    
    Usa:
    - postgres_datos_externos: Para el catálogo de metadatos (PostgreSQL)
    - rustfs_s3_conn: Para almacenar los datos (RustFS/S3)
    """
    from airflow.hooks.base import BaseHook # type: ignore
    
    # Obtener configuración de PostgreSQL desde Airflow
    pg_conn = BaseHook.get_connection('postgres_datos_externos')
    POSTGRES_HOST = pg_conn.host
    POSTGRES_PORT = pg_conn.port
    POSTGRES_DB = pg_conn.schema
    POSTGRES_USER = pg_conn.login
    POSTGRES_PASSWORD = pg_conn.password
    
    # Obtener configuración de RustFS desde Airflow
    s3_conn = BaseHook.get_connection('rustfs_s3_conn')
    s3_extra = s3_conn.extra_dejson
    S3_ENDPOINT = s3_extra.get('endpoint_url', 'http://rustfs:9000').replace('http://', '').replace('https://', '')
    
    # Las credenciales AWS están en extra_dejson
    RUSTFS_USER = s3_extra.get('aws_access_key_id', 'admin')
    RUSTFS_PASSWORD = s3_extra.get('aws_secret_access_key', 'muceim-duckduck.2025!')
    RUSTFS_BUCKET = os.getenv('RUSTFS_BUCKET', 'mitma')
    RUSTFS_SSL = 'true' if 'https' in s3_extra.get('endpoint_url', '') else 'false'
    
    con = duckdb.connect()
    
    # Instalar y cargar extensiones necesarias
    con.execute("""
        INSTALL ducklake;
        LOAD ducklake;
        INSTALL postgres;
        LOAD postgres;
        INSTALL httpfs;
        LOAD httpfs;
        INSTALL spatial;
        LOAD spatial;
    """)
    
    # Configurar credenciales S3 para RustFS
    con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
    con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
    con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
    con.execute("SET s3_url_style='path';")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET max_temp_directory_size='40GiB';")
    
    # Attach DuckLake con catálogo PostgreSQL y datos en RustFS
    postgres_connection_string = f"dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}"
    attach_query = f"""
        ATTACH 'ducklake:postgres:{postgres_connection_string}' AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
    """
    con.execute(attach_query)
    
    con.execute("USE ducklake;")
    
    return con
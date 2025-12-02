"""
Utility functions for Bronze layer data ingestion from MITMA.
Contains helper functions for DuckDB operations, URL fetching, and data merging.
"""

import re
import urllib.request
import requests
import pandas as pd
import geopandas as gpd
import duckdb
import os
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv('.env', override=True)


# ==========================================
# CONNECTION MANAGEMENT
# ==========================================

class DuckLakeConnectionManager:
    """
    Singleton manager for DuckLake connections.
    Ensures only one connection is created and reused.
    """
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckLakeConnectionManager, cls).__new__(cls)
        return cls._instance
    
    def get_connection(self, force_new=False):
        """
        Get or create a DuckLake connection.
        
        Parameters:
        - force_new: If True, close existing connection and create new one
        
        Returns:
        - DuckDB connection object
        """
        if force_new and self._connection is not None:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None
        
        if self._connection is None:
            self._connection = self._create_connection()
        
        return self._connection
    
    def _create_connection(self):
        """Create a new DuckLake connection with RustFS and Postgres."""
        
        RUSTFS_HOST = os.getenv('RUSTFS_HOST', 'localhost')
        RUSTFS_PORT = os.getenv('RUSTFS_PORT', '9000')
        RUSTFS_USER = os.getenv('RUSTFS_USER', 'admin')
        RUSTFS_PASSWORD = os.getenv('RUSTFS_PASSWORD', 'password')
        RUSTFS_BUCKET = os.getenv('RUSTFS_BUCKET', 'mitma')
        RUSTFS_SSL = os.getenv('RUSTFS_SSL', 'false')
        
        POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
        POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
        POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
        POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
        POSTGRES_DB = os.getenv('POSTGRES_DB', 'muceim')
        
        S3_ENDPOINT = f"{RUSTFS_HOST}:{RUSTFS_PORT}"
        
        # Create connection
        con = duckdb.connect()
        
        # Install extensions
        con.execute("""
            INSTALL ducklake;
            LOAD ducklake;
            INSTALL postgres;
            LOAD postgres;
            INSTALL httpfs;
            LOAD httpfs;
        """)
        
        # Configure S3 for RustFS
        con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
        con.execute(f"SET s3_access_key_id='{RUSTFS_USER}';")
        con.execute(f"SET s3_secret_access_key='{RUSTFS_PASSWORD}';")
        con.execute(f"SET s3_use_ssl={RUSTFS_SSL};")
        con.execute("SET s3_url_style='path';")
        con.execute("SET preserve_insertion_order=false;")
        con.execute("SET max_temp_directory_size='40GiB';")
        
        # Check if ducklake is already attached
        databases = con.execute("SELECT database_name FROM duckdb_databases();").fetchdf()
        if 'ducklake' not in databases['database_name'].values:
            # Attach DuckLake with Postgres Catalog
            postgres_connection_string = f"dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} password={POSTGRES_PASSWORD} port={POSTGRES_PORT}"
            attach_query = f"""
                ATTACH 'ducklake:postgres:{postgres_connection_string}' AS ducklake (DATA_PATH 's3://{RUSTFS_BUCKET}/');
            """
            con.execute(attach_query)
        
        con.execute("USE ducklake;")
        
        return con
    
    def close(self):
        """Close the connection if it exists."""
        if self._connection is not None:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None


# Global connection manager instance
_connection_manager = DuckLakeConnectionManager()


def get_ducklake_connection(force_new=False):
    """
    Get a reusable DuckLake connection (Singleton pattern).
    
    This is the recommended way to get a connection in your tasks.
    The same connection is reused across calls to avoid duplicate ATTACH errors.
    
    Parameters:
    - force_new: If True, close existing connection and create new one
    
    Returns:
    - DuckDB connection object
    
    Example:
        con = get_ducklake_connection()
        con.execute("SELECT * FROM bronze_mitma_od_distritos LIMIT 10")
        # Don't close it - it will be reused!
    """
    return _connection_manager.get_connection(force_new=force_new)


@contextmanager
def ducklake_connection():
    """
    Context manager for DuckLake connection.
    
    Use this when you want automatic cleanup, but be aware it will
    close the connection when exiting the context.
    
    Example:
        with ducklake_connection() as con:
            con.execute("SELECT * FROM bronze_mitma_od_distritos")
    """
    con = get_ducklake_connection()
    try:
        yield con
    finally:
        # Don't close here - let the manager handle it
        pass


def close_ducklake_connection():
    """
    Explicitly close the DuckLake connection.
    Only use this at the very end of your pipeline.
    """
    _connection_manager.close()
    
import tempfile
import os
import io
from datetime import datetime
from shapely.validation import make_valid


def get_mitma_urls(dataset, zone_type, start_date, end_date):
    """
    Fetches MITMA URLs from RSS feed and filters by dataset, zone type, and date range.
    
    Parameters:
    - dataset: 'od', 'people_day', 'overnight_stay'
    - zone_type: 'distritos', 'municipios', 'gau'
    - start_date: 'YYYY-MM-DD'
    - end_date: 'YYYY-MM-DD'
    
    Returns:
    - List of URLs matching the criteria
    """
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"
    
    # Simple mapping: dataset -> (url_path, file_prefix)
    dataset_map = {
        "od": ("viajes", "Viajes"),
        "people_day": ("personas", "Personas_dia"),
        "overnight_stay": ("pernoctaciones", "Pernoctaciones")
    }
    
    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")
    if dataset not in dataset_map:
        raise ValueError(f"Invalid dataset: {dataset}. Must be one of {list(dataset_map.keys())}.")
    
    dataset_path, file_prefix = dataset_map[dataset]
    
    # Construct file pattern: {Prefix}_{zone} (GAU is uppercase in files)
    zone_suffix = "GAU" if zone_type == "gau" else zone_type
    file_pattern = f"{file_prefix}_{zone_suffix}"
    
    # Build dynamic regex pattern
    # Pattern: https://.../por-{zone}/viajes/ficheros-diarios/YYYY-MM/YYYYMMDD_{FilePattern}.csv.gz
    pattern = rf'(https?://[^\s"<>]*/estudios_basicos/por-{zone_type}/{dataset_path}/ficheros-diarios/\d{{4}}-\d{{2}}/(\d{{8}})_{file_pattern}\.csv\.gz)'
        
    # Fetch RSS with User-Agent to avoid 403
    req = urllib.request.Request(rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")
    
    # Find all matches (case-insensitive for por-gau vs por-GAU)
    matches = re.findall(pattern, txt, re.I)
    
    # Remove duplicates using set (RSS often has duplicate entries)
    unique_matches = list(set(matches))
    
    # Convert date range to comparable format
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Filter by date range and sort
    filtered_urls = []
    for url, date_str in unique_matches:
        file_date = datetime.strptime(date_str, "%Y%m%d")
        if start_dt <= file_date <= end_dt:
            filtered_urls.append((url, date_str))
    
    # Sort by date ascending
    filtered_urls.sort(key=lambda x: x[1])
    
    # Extract just the URLs
    urls = [url for url, _ in filtered_urls]
    
    print(f"Found {len(urls)} URLs for {dataset} {zone_type} from {start_date} to {end_date}")
    
    if not urls:
        print(f"WARNING: No URLs found. Check if data exists for the requested date range.")
    
    return urls


def create_and_merge_table(con, dataset, zone_type, urls, lake_layer='bronze'):
    """
    Generic function to create table and merge data for any MITMA dataset.
    Uses ALL columns from the CSV as merge keys (bronze layer pattern).

    Parameters:
    - con: DuckDB connection
    - dataset: 'od', 'people_day', 'overnight_stay'
    - zone_type: 'distritos', 'municipios', 'gau'
    - urls: list of URLs to load
    - lake_layer: layer name (default: 'bronze')
    """
    if dataset:
        table_name = f'{lake_layer}_mitma_{dataset}_{zone_type}'
    else:
        table_name = f'{lake_layer}_mitma_{zone_type}'
    
    # Convert list of URLs to a string representation for DuckDB list
    url_list_str = "[" + ", ".join([f"'{u}'" for u in urls]) + "]"

    # Step 1: Create table if not exists (using first file for schema inference)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT 
            * EXCLUDE (filename),
            CURRENT_TIMESTAMP AS loaded_at,
            filename AS source_file
        FROM read_csv(
            {url_list_str},
            filename = true,
            all_varchar = true
        )
        LIMIT 0;
    """)
    
    # Get column names from the table (excluding audit columns)
    columns_df = con.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        AND column_name NOT IN ('loaded_at', 'source_file')
        ORDER BY ordinal_position;
    """).fetchdf()
    
    merge_keys = columns_df['column_name'].tolist()
    
    # Build ON clause from all CSV columns
    on_clause = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
    
    # Step 3: MERGE for idempotent incremental loads
    con.execute(f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT 
                * EXCLUDE (filename),
                CURRENT_TIMESTAMP AS loaded_at,
                filename AS source_file
            FROM read_csv(
                {url_list_str},
                filename = true,
                all_varchar = true
            )
        ) AS source
        ON {on_clause}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *;
    """)
    
    print(f"Table {table_name} merged successfully with {len(merge_keys)} key columns.")


def get_mitma_zoning_urls(zone_type):
    """
    Fetches MITMA Zoning URLs (Shapefiles + CSVs) from RSS feed using Regex.
    Matches the style of 'get_mitma_urls' but for static zoning files.
    
    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau'
    
    Returns:
    - Dictionary with shapefile components, nombres URL, and poblacion URL
    """
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"
    
    # Normalización de input
    if zone_type not in ["distritos", "municipios", "gau"]:
        raise ValueError(f"Invalid zone_type: {zone_type}. Must be 'distritos', 'municipios', or 'gau'.")

    # Lógica de sufijos para construir el Regex
    # Carpeta en URL: zonificacion_municipios | zonificacion_distritos | zonificacion_GAU
    folder_suffix = "GAU" if zone_type == "gau" else zone_type
    
    # Sufijo en ficheros CSV: nombres_municipios | nombres_distritos | nombres_gaus
    file_suffix = "gaus" if zone_type == "gau" else zone_type
    
    # --- REGEX PATTERNS ---
    # 1. Pattern para componentes del Shapefile (.shp, .shx, .dbf, .prj)
    # Busca URLs que contengan /zonificacion_{Suffix}/ y terminen en extensión de shapefile
    shp_pattern = rf'(https?://[^\s"<>]*/zonificacion/zonificacion_{folder_suffix}/[^"<>]+\.(?:shp|shx|dbf|prj))'
    
    # 2. Pattern para CSVs auxiliares (nombres_*.csv, poblacion_*.csv)
    # Busca URLs que contengan /zonificacion_{Suffix}/ y sean nombres_X.csv o poblacion_X.csv
    csv_pattern = rf'(https?://[^\s"<>]*/zonificacion/zonificacion_{folder_suffix}/(?:nombres|poblacion)_{file_suffix}\.csv)'

    print(f"📡 Scanning RSS for {zone_type} zoning files...")

    try:
        # Fetch RSS with User-Agent
        req = urllib.request.Request(rss_url, headers={"User-Agent": "MITMA-DuckLake-Loader"})
        with urllib.request.urlopen(req) as response:
            txt = response.read().decode("utf-8", "ignore")
        
        # Find matches
        shp_matches = re.findall(shp_pattern, txt, re.IGNORECASE)
        csv_matches = re.findall(csv_pattern, txt, re.IGNORECASE)
        
        # Deduplicate
        unique_shp = sorted(list(set(shp_matches)))
        unique_csv = sorted(list(set(csv_matches)))
        
        # Organizar resultados
        url_nombres = next((u for u in unique_csv if 'nombres' in u.lower()), None)
        url_poblacion = next((u for u in unique_csv if 'poblacion' in u.lower()), None)
        
        if not unique_shp and not unique_csv:
            print("WARNING: No zoning URLs found in RSS. The feed might have rotated them out.")
            return {}

        print(f"Found {len(unique_shp)} shapefile components and {len(unique_csv)} CSVs.")
        
        return {
            "shp_components": unique_shp,
            "nombres": url_nombres,
            "poblacion": url_poblacion
        }

    except Exception as e:
        print(f"ERROR fetching RSS: {e}")
        return {}


def clean_id(series):
    """Normaliza ID a string limpio (sin .0, sin espacios)."""
    return series.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)


def clean_poblacion(series):
    """Limpia enteros de población (quita puntos y decimales)."""
    return (series.astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(r'\.0$', '', regex=True)
            .apply(pd.to_numeric, errors='coerce')
            .fillna(0).astype(int))


def get_mitma_zoning_dataset(zone_type='municipios'):
    """
    Orquesta la descarga, limpieza y fusión de datos maestros.
    Retorna un GeoDataFrame listo para ingesta.
    
    Parameters:
    - zone_type: 'distritos', 'municipios', 'gau'
    
    Returns:
    - GeoDataFrame with zoning data
    """
    urls = get_mitma_zoning_urls(zone_type)
    
    print(f"🚀 Generando dataset maestro para: {zone_type.upper()}")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        print("   ⬇️  Descargando geometrías...")
        shp_local_path = None
        
        for url in urls['shp_components']:
            filename = url.split('/')[-1]
            try:
                r = requests.get(url, timeout=15)
                if r.status_code == 200:
                    local_p = os.path.join(tmp_dir, filename)
                    with open(local_p, 'wb') as f:
                        f.write(r.content)
                    if filename.endswith('.shp'):
                        shp_local_path = local_p
            except Exception as e:
                print(f"      ⚠️ Error bajando {filename}: {e}")

        if not shp_local_path:
            print("❌ Error: No se pudo descargar el archivo .shp principal.")
            return None

        gdf = gpd.read_file(shp_local_path)
        
        id_col = next((c for c in gdf.columns if c.upper() in ['ID', 'CODIGO', 'ZONA', 'COD_GAU']), 'ID')
        gdf['ID'] = clean_id(gdf[id_col])
        
        gdf['geometry'] = gdf['geometry'].apply(make_valid)
        if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")

        print("   🔗 Integrando metadatos (Nombres y Población)...")
        df_aux = pd.DataFrame(columns=['ID'])
        
        aux_config = [
            {
                'type': 'nombres', 
                'url': urls['nombres'], 
                'header': 0, 
                'cols': ['ID', 'Nombre']
            },
            {
                'type': 'poblacion', 
                'url': urls['poblacion'], 
                'header': None, 
                'cols': ['ID', 'Poblacion']
            }
        ]

        for cfg in aux_config:
            try:
                r = requests.get(cfg['url'], timeout=10)
                if r.status_code == 200:
                    # Leer CSV crudo
                    df_t = pd.read_csv(
                        io.BytesIO(r.content), 
                        sep='|', 
                        header=cfg['header'], 
                        dtype=str, 
                        engine='python'
                    )
                    

                    if len(df_t.columns) >= 3:
                        df_t = df_t.iloc[:, [1, 2]]
                    elif len(df_t.columns) == 2:
                        df_t = df_t.iloc[:, [0, 1]]
                    
                    df_t.columns = cfg['cols']
                    
                    df_t['ID'] = clean_id(df_t['ID'])
                    df_t = df_t.drop_duplicates(subset=['ID'])
                    
                    if cfg['type'] == 'poblacion':
                        df_t['Poblacion'] = clean_poblacion(df_t['Poblacion'])

                    if df_aux.empty:
                        df_aux = df_t
                    else:
                        df_aux = df_aux.merge(df_t, on='ID', how='outer')
                        
                    print(f"      ✓ {cfg['type'].capitalize()} OK")
            except Exception as e:
                print(f"      ⚠️ Fallo procesando {cfg['type']}: {e}")

        # --- C. Merge Final ---
        if not df_aux.empty:
            gdf = gdf.merge(df_aux, on='ID', how='left')
            
            if 'Nombre' in gdf.columns: 
                gdf['Nombre'] = gdf['Nombre'].fillna(gdf['ID'])
            if 'Poblacion' in gdf.columns: 
                gdf['Poblacion'] = gdf['Poblacion'].fillna(0).astype(int)

        cols = ['ID', 'Nombre', 'Poblacion', 'geometry']
        final_cols = [c for c in cols if c in gdf.columns] + [c for c in gdf.columns if c not in cols]
        gdf = gdf[final_cols]

        print(f"✅ Dataset generado: {len(gdf)} registros.")
        return gdf


def load_zonificacion(con, zone_type, lake_layer='bronze'):
    """
    Load zonification data into DuckDB for the specified type.
    
    Parameters:
    - con: DuckDB connection
    - zone_type: 'distritos', 'municipios', 'gau'
    - lake_layer: layer name (default: 'bronze')
    """
    df = get_mitma_zoning_dataset(zone_type)
    
    if df is None or df.empty:
        print(f"No data to load for {zone_type}")
        return
    
    # Convert all columns to string (including geometry)
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    table_name = f'{lake_layer}_mitma_{zone_type}'
    
    con.register('temp_zonificacion', df)
    
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT
            *,
            CURRENT_TIMESTAMP AS loaded_at,
        FROM temp_zonificacion
        LIMIT 0;
    """)
    
    merge_key = 'ID'
    
    con.execute(f"""
        MERGE INTO {table_name} AS target
        USING (
            SELECT
                *,
                CURRENT_TIMESTAMP AS loaded_at,
            FROM temp_zonificacion
        ) AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *;
    """)
    
    con.unregister('temp_zonificacion')
    
    print(f"Table {table_name} merged successfully with {len(df)} records.")

import re
import urllib.request
from datetime import datetime
from airflow.sdk import  dag, chain, task # type: ignore


from utils import connect_datalake


@task
def fetch_mitma_urls(data_type, zone_level, time_period):
    """
    Fetches the RSS feed and filters for URLs containing the DAG parameter.
    """
    if data_type not in ['pernoctaciones', 'personas', 'viajes']:
        assert ValueError("Invalid data_type. ['pernoctaciones', 'personas', 'viajes']")
    if zone_level not in ['distritos', 'municipios', 'GAU']:
        assert ValueError("Invalid zone_level. ['distritos', 'municipios', 'GAU']")

    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"
    # Regex to capture the full URL and the date component
    pattern = rf'(https://movilidad-opendata.mitma.es/estudios_basicos/por-{zone_level}/{data_type}/ficheros-diarios/{time_period}/(\d{{8}})_{data_type.capitalize()}_{zone_level}\.csv\.gz)'
    print(f"Fetching RSS from {rss_url}...")

    # Open request with User-Agent to avoid 403 Forbidden
    req = urllib.request.Request(rss_url, headers={"User-Agent": "MITMA-RSS-parser"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")


    matches = re.findall(pattern, txt, re.I)
    urls = sorted(set(matches), key=lambda x: x[1], reverse=True)
    print(f"Found {len(urls)} URLs matching for '{data_type}' data at '{zone_level}' zone level for '{time_period}'.")
    return [{"url": url[0], "zone_type": zone_level} for url in urls[:2]]

@task(
    max_active_tis_per_dag=1
)
def ingest_viajes_mitma(url_dict):
    url = url_dict["url"]
    zone_type = url_dict["zone_type"]
    
    table_name = f"bronze_od_{zone_type}"
    con = connect_datalake()

    try:
        # Install and load 
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Load data
        con.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                fecha TEXT,
                periodo TEXT,
                origen TEXT,
                destino TEXT,
                distancia TEXT,
                actividad_origen TEXT,
                actividad_destino TEXT,
                residencia TEXT,
                renta TEXT,
                edad TEXT,
                sexo TEXT,
                viajes TEXT,
                viajes_km TEXT,
                estudio_destino_posible TEXT,
                estudio_origen_posible TEXT,
                loaded_at TIMESTAMP,
                source_file TEXT
            );
        """)

        con.execute(f"""
            INSERT INTO {table_name} (
                fecha, periodo, origen, destino, distancia,
                actividad_origen, actividad_destino,
                residencia, renta, edad, sexo,
                viajes, viajes_km, estudio_destino_posible,
                estudio_origen_posible, loaded_at, source_file
            )
            SELECT
                fecha,
                periodo,
                origen,
                destino,
                distancia,
                actividad_origen,
                actividad_destino,
                residencia,
                renta,
                edad,
                sexo,
                viajes,
                viajes_km,
                estudio_destino_posible,
                estudio_origen_posible,
                CURRENT_TIMESTAMP AS loaded_at,
                filename AS source_file
            FROM read_csv(
                '{url}',
                filename = true,
                all_varchar = true,
                delim = '|'
            );
        """)

        print(f"Successfully loaded: {url}")

    except Exception as e:
        print(f"Error processing {url}: {e}")
        raise e
    finally:
        con.close()


@dag(
    dag_id="mitma_viajes_ingest",
    start_date=datetime(2025, 11, 27),
    schedule=None,  
    catchup=False,
)
def mitma_viajes_ingest_dag():
    # 1. Fetch URLs
    urls = fetch_mitma_urls("viajes", "municipios", "2023-01")

    # 2. Take first 2 and map ingest
    ingested = ingest_viajes_mitma.expand(
        url_dict=urls
    )


mitma_viajes_ingest_dag()

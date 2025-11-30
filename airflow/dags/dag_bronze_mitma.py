import os
import re
import urllib.request
from datetime import datetime
from pyspainmobility import Zones
from airflow.sdk import  dag, chain, task # type: ignore
from airflow.models.param import Param # type: ignore


from utils import connect_datalake


@task
def generate_search_parameters(data_type: str, year: int, zone):
    parameters = []
    for month in range(1, 13):
        parameters.append({
            "data_type": data_type,
            "zone": zone,
            "time_period": f"{year}-{month:02}",
        })
    return parameters

@task
def fetch_mitma_urls(search_dict):
    """
    Fetches the RSS feed and filters for URLs containing the DAG parameter.
    """
    data_type = search_dict["data_type"]
    zone_level = search_dict["zone"]
    time_period = search_dict["time_period"]

    if data_type not in ['pernoctaciones', 'personas', 'viajes']:
        assert ValueError("Invalid data_type. ['pernoctaciones', 'personas', 'viajes']")
    if zone_level not in ['distritos', 'municipios', 'GAU']:
        assert ValueError("Invalid zone_level. ['distritos', 'municipios', 'GAU']")

    if data_type == "personas":
        name_type = f"{data_type}_dia".capitalize()
    else:
        name_type = data_type.capitalize()
    rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"
    # Regex to capture the full URL and the date component
    pattern = rf'(https://movilidad-opendata.mitma.es/estudios_basicos/por-{zone_level}/{data_type}/ficheros-diarios/{time_period}/(\d{{8}})_{name_type}_{zone_level}\.csv\.gz)'
    print(f"Fetching RSS from {rss_url}...")

    # Open request with User-Agent to avoid 403 Forbidden
    req = urllib.request.Request(rss_url, headers={"User-Agent": "MITMA-RSS-parser"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")


    matches = re.findall(pattern, txt, re.I)
    urls = sorted(set(matches), key=lambda x: x[1], reverse=False)
    print(f"Found {len(urls)} URLs matching for '{data_type}' data at '{zone_level}' zone level for '{time_period}'.")
    return [{"url": url[0], "zone_type": zone_level} for url in urls]

@task
def flatten(list_of_lists):
    out = []
    for lst in list_of_lists:
        out.extend(lst)
    return out



######################################################
##                  VIAJES
######################################################


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
    params={
        "year": Param(2023, type="integer", minimum=2022, maximum=2025),
        "zone": Param(
            "distritos",
            type="string",
            enum=["distritos", "municipios", "GAU"]
        )
    }
)
def mitma_viajes_ingest_dag(year, zone):
    monthly_params = generate_search_parameters(
        data_type="viajes",
        year=year,
        zone=zone
    )

    urls = fetch_mitma_urls.expand(
        search_dict = monthly_params  # ← esto sí funciona
    )

    flatten_url = flatten(
        list_of_lists = urls
    )

    ingest_viajes_mitma.expand(
        url_dict = flatten_url
    )

mitma_viajes_ingest_dag(2023, 'distritos')


######################################################
##                  PERNOCTACIONES
######################################################


@task(
    max_active_tis_per_dag=1
)
def ingest_pernoctaciones_mitma(url_dict):
    url = url_dict["url"]
    zone_type = url_dict["zone_type"]
    
    table_name = f"bronze_pernoctaciones_{zone_type}"
    con = connect_datalake()

    try:
        # Install and load 
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Load data
        con.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                fecha TEXT,
                zona_residencia TEXT,
                zona_pernoctacion TEXT,
                personas TEXT,
                loaded_at TIMESTAMP,
                source_file TEXT
            );
        """)

        con.execute(f"""
            INSERT INTO {table_name} (
                fecha, zona_residencia, zona_pernoctacion, personas, loaded_at, source_file
            )
            SELECT
                fecha,
                zona_residencia,
                zona_pernoctacion,
                personas,
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
    dag_id="mitma_pernoctaciones_ingest",
    start_date=datetime(2025, 11, 27),
    schedule=None,
    catchup=False,
    params={
        "year": Param(2023, type="integer", minimum=2022, maximum=2025),
        "zone": Param(
            "distritos",
            type="string",
            enum=["distritos", "municipios", "GAU"]
        )
    }
)
def mitma_pernoctaciones_ingest_dag(year, zone):
    search_params = generate_search_parameters("pernoctaciones", year, zone)

    urls = fetch_mitma_urls.expand(
        search_dict = search_params
    )

    flatten_url = flatten(
        list_of_lists = urls
    )

    ingest_pernoctaciones_mitma.expand(
        url_dict = flatten_url
    )


mitma_pernoctaciones_ingest_dag(2023, 'distritos')





######################################################
##                  PERSONAS
######################################################


@task(
    max_active_tis_per_dag=1
)
def ingest_personas_mitma(url_dict):
    url = url_dict["url"]
    zone_type = url_dict["zone_type"]
    
    table_name = f"bronze_personas_{zone_type}"
    con = connect_datalake()

    try:
        # Install and load 
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Load data
        con.sql(f"""
            CREATE TABLE {table_name}(
                fecha TEXT,
                zona_pernoctacion TEXT,
                edad TEXT,
                sexo TEXT,
                numero_viajes TEXT,
                personas TEXT,
                loaded_at TIMESTAMP,
                source_file TEXT
            );
        """)

        con.execute(f"""
            INSERT INTO {table_name} (
                fecha, zona_pernoctacion, edad, sexo, numero_viajes, personas, loaded_at, source_file
            )
            SELECT
                fecha,
                zona_pernoctacion,
                edad,
                sexo,
                numero_viajes,
                personas,
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
    dag_id="mitma_personas_ingest",
    start_date=datetime(2025, 11, 27),
    schedule=None,  
    catchup=False,
    params={
        "year": Param(2023, type="integer", minimum=2022, maximum=2025),
        "zone": Param(
            "distritos",
            type="string",
            enum=["distritos", "municipios", "GAU"]
        )
    }
)
def mitma_personas_ingest_dag(year, zone):
    search_params = generate_search_parameters("personas", year, zone)

    urls = fetch_mitma_urls.expand(
        search_dict = search_params
    )

    flatten_url = flatten(
        list_of_lists = urls
    )

    ingest_personas_mitma.expand(
        url_dict = flatten_url
    )


mitma_personas_ingest_dag(2023, 'distritos')


######################################################
##                  ZONAS
######################################################

@task
def fetch_zonificacion_path(zone_level):
    if zone_level not in ['distritos', 'municipios', 'gau']:
        assert ValueError("Invalid zone_level. ['distritos', 'municipios', 'gau']")

    save_path = f"include/resources/MITMA/{zone_level}.csv.gz"
    zones = Zones(
        version=2,
        zones=zone_level
    )
    df = zones.get_zone_geodataframe()

    if not os.path.exists(save_path):
        df.to_csv(save_path, index=True, compression="gzip")

    return {"path": save_path, "zone_type": zone_level}

@task(
    max_active_tis_per_dag=1
)
def ingest_zones_mitma(path_dict):
    path = path_dict["path"]
    zone_type = path_dict["zone_type"]
    
    table_name = f"bronze_zones_{zone_type}"
    con = connect_datalake()

    try:
        con.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            id TEXT,
            name TEXT,
            population TEXT,
            geometry TEXT,   -- stored as plain text in BRONZE layer
            loaded_at TIMESTAMP,
            source_file TEXT
            );
        """)
        con.execute(f"""
            INSERT INTO {table_name} (
                id, name, population, geometry, loaded_at, source_file
            )
            SELECT
                id,
                name,
                population,
                geometry,
                CURRENT_TIMESTAMP AS loaded_at,
                filename AS source_file
            FROM read_csv(
                '{path}',
                filename = true,
                all_varchar = true
            );
        """)

        print(f"Successfully loaded: {path}")

    except Exception as e:
        print(f"Error processing {path}: {e}")
        raise e
    finally:
        con.close()

@dag(
    dag_id="mitma_zonas_ingest",
    start_date=datetime(2025, 11, 27),
    schedule=None,  
    catchup=False,
    params={
        "zone": Param(
            "distritos",
            type="string",
            enum=["distritos", "municipios", "gau"]
        )
    }
)
def mitma_zonas_ingest_dag(zone):
    path = fetch_zonificacion_path(zone)

    ingest_zones_mitma(
        path_dict = path
    )


mitma_zonas_ingest_dag("distritos")
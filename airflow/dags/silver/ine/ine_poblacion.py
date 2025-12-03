import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_ine_poblacion():
    print("[TASK] Building silver_business unified table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_population AS
        WITH poblacion_flat AS (
            SELECT 
                p.COD,
                CAST(epoch_ms(CAST(data_item.Fecha AS BIGINT)) AS DATE) + INTERVAL 1 DAY AS fecha,
                COALESCE(NULLIF(TRIM(split_part(p.Nombre, '.', 1)), ''), p.Nombre) AS nombre,
                LOWER(TRIM(split_part(p.Nombre, '.', 2))) AS tipo,
                CAST(data_item.Valor AS DOUBLE) AS valor
            FROM bronze_ine_poblacion_municipio p,
                UNNEST(p.Data) AS t(data_item)
            WHERE p.Data IS NOT NULL 
            AND len(p.Data) > 0
        )
        SELECT DISTINCT ON (pf.COD)
            m.Codigo AS codigo_ine,
            pf.* EXCLUDE (COD),
            r.distrito_mitma,
            r.municipio_mitma,
            r.gau_mitma
        FROM poblacion_flat pf
        LEFT JOIN bronze_ine_municipios m 
            ON pf.nombre ILIKE m.Nombre
        LEFT JOIN bronze_mitma_ine_relations r
            ON m.Codigo = r.municipio_ine
        WHERE (
            r.distrito_mitma IS NOT NULL 
            OR r.municipio_mitma IS NOT NULL 
            OR r.gau_mitma IS NOT NULL
        );
    """)

    # Count & preview
    df = con.execute("SELECT COUNT(*) AS count FROM silver_population").fetchdf()
    print(f"[TASK] Created silver_population with {df.iloc[0]['count']:,} records")

    print("[TASK] Sample preview:")
    print(con.execute("SELECT * FROM silver_population LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(df.iloc[0]["count"]),
        "table": "silver_population"
    }
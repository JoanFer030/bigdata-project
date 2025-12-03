import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_ine_renta():
    print("[TASK] Building silver_income unified table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_income AS
        WITH renta_flat AS (
            SELECT 
                r.COD,
                CAST(epoch_ms(CAST(data_item.Fecha AS BIGINT)) AS DATE) + INTERVAL 1 DAY AS fecha,
                -- Limpiar el nombre: extraer solo el nombre de la población
                TRIM(regexp_replace(
                    split_part(r.Nombre, '.', 1),
                    '\s+(distrito|sección|seccion)\s+\d+',
                    '',
                    'gi'
                )) AS nombre,
                LOWER(TRIM(split_part(r.Nombre, '.', 3))) AS tipo,
                CAST(data_item.Valor AS DOUBLE) AS valor
            FROM bronze_ine_renta_municipio r,
                UNNEST(r.Data) AS t(data_item)
            WHERE r.Data IS NOT NULL 
            AND len(r.Data) > 0
            -- Filtrar solo registros de municipio (sin distrito ni sección)
            AND NOT regexp_matches(LOWER(r.Nombre), '(sección|seccion)\s+\d+')
            AND NOT regexp_matches(LOWER(r.Nombre), 'distrito\s+\d+')
        )
        SELECT DISTINCT ON (rf.COD)
            m.Codigo AS codigo_ine,
            rf.* EXCLUDE (COD),
            rel.distrito_mitma,
            rel.municipio_mitma,
            rel.gau_mitma
        FROM renta_flat rf
        LEFT JOIN bronze_ine_municipios m 
            ON rf.nombre ILIKE m.Nombre
        LEFT JOIN bronze_mitma_ine_relations rel
            ON m.Codigo = rel.municipio_ine
        WHERE valor IS NOT NULL AND (
            distrito_mitma IS NOT NULL 
            OR municipio_mitma IS NOT NULL 
            OR gau_mitma IS NOT NULL
        )
    """)

    # Count & preview
    df = con.execute("SELECT COUNT(*) AS count FROM silver_income").fetchdf()
    print(f"[TASK] Created silver_income with {df.iloc[0]['count']:,} records")

    print("[TASK] Sample preview:")
    print(con.execute("SELECT * FROM silver_income LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(df.iloc[0]["count"]),
        "table": "silver_income"
    }


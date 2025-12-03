import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_ine_empresas():
    print("[TASK] Building silver_business unified table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_business AS
        WITH empresas_flat AS (
            SELECT 
                e.COD,
                CAST(epoch_ms(CAST(data_item.Fecha AS BIGINT)) AS DATE) + INTERVAL 1 DAY AS fecha,
                COALESCE(NULLIF(TRIM(split_part(e.Nombre, '.', 1)), ''), e.Nombre) AS nombre,
                TRIM(split_part(e.Nombre, '.', 4)) AS tipo,
                CAST(data_item.Valor AS DOUBLE) AS valor
            FROM bronze_ine_empresas_municipio e,
                UNNEST(e.Data) AS t(data_item)
            WHERE e.Data IS NOT NULL 
            AND len(e.Data) > 0
        )
        SELECT DISTINCT ON (ef.COD)
            m.Codigo AS codigo_ine,
            ef.* EXCLUDE (COD),
            r.distrito_mitma,
            r.municipio_mitma,
            r.gau_mitma
        FROM empresas_flat ef
        LEFT JOIN bronze_ine_municipios m 
            ON ef.Nombre ILIKE m.Nombre
        LEFT JOIN bronze_mitma_ine_relations r
            ON m.Codigo = r.municipio_ine
        WHERE ef.Tipo ILIKE '%CNAE%' AND (
            r.distrito_mitma IS NOT NULL 
            OR r.municipio_mitma IS NOT NULL 
            OR r.gau_mitma IS NOT NULL
        )
    """)

    # Count & preview
    df = con.execute("SELECT COUNT(*) AS count FROM silver_business").fetchdf()
    print(f"[TASK] Created silver_business with {df.iloc[0]['count']:,} records")

    print("[TASK] Sample preview:")
    print(con.execute("SELECT * FROM silver_business LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(df.iloc[0]["count"]),
        "table": "silver_business"
    }

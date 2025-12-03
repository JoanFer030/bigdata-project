"""
Airflow task for loading MITMA overnight stay data into Silver layer.
Handles overnight stay counts for distritos, municipios, and GAU zone types,
with proper type casting, zone_level labeling, and filtering of incomplete rows.
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


from utils import get_ducklake_connection


@task
def SILVER_mitma_overnight_stay():
    """
    Airflow task to transform and standardize overnight stay data into DuckDB Silver layer.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building unified silver_overnight_stay table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_overnight_stay AS
        WITH base AS (
            -- Distritos
            SELECT
                'distrito' AS zone_level,
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE AS date,
                zona_pernoctacion AS overnight_zone,
                zona_residencia AS residence_zone,
                CAST(personas AS DOUBLE) AS people
            FROM bronze_mitma_overnight_stay_distritos

            UNION ALL

            -- Municipios
            SELECT
                'municipio',
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE,
                zona_pernoctacion,
                zona_residencia,
                CAST(personas AS DOUBLE)
            FROM bronze_mitma_overnight_stay_municipios

            UNION ALL

            -- GAU
            SELECT
                'gau',
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE,
                zona_pernoctacion,
                zona_residencia,
                CAST(personas AS DOUBLE)
            FROM bronze_mitma_overnight_stay_gau
        ),
        filtered AS (
            SELECT *
            FROM base
            WHERE date IS NOT NULL
              AND overnight_zone IS NOT NULL
              AND residence_zone IS NOT NULL
              AND people IS NOT NULL
        )
        SELECT * FROM filtered;
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_overnight_stay").fetchdf()
    print(f"[TASK] Created silver_overnight_stay with {count.iloc[0]['count']:,} records")

    print(con.execute("SELECT * FROM silver_overnight_stay LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_overnight_stay"
    }

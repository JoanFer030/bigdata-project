"""
Airflow task for loading MITMA People Day data into Silver layer.
Handles daily person-trip counts (personas) by zone, age, sex, and travel type,
for distritos, municipios, and GAU zone types.
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


from utils import get_ducklake_connection


@task
def SILVER_mitma_people_day():
    """
    Airflow task to transform and standardize MITMA People Day data
    into DuckDB Silver layer.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building unified silver_people_day table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_people_day AS
        WITH base AS (
            -- Distritos
            SELECT
                'distrito' AS zone_level,
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE AS date,
                zona_pernoctacion AS overnight_zone,
                edad AS age,
                sexo AS sex,
                numero_viajes AS n_trips,
                CAST(personas AS DOUBLE) AS people
            FROM bronze_mitma_people_day_distritos

            UNION ALL

            -- Municipios
            SELECT
                'municipio',
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE,
                zona_pernoctacion,
                edad,
                sexo,
                numero_viajes,
                CAST(personas AS DOUBLE)
            FROM bronze_mitma_people_day_municipios

            UNION ALL

            -- GAU
            SELECT
                'gau',
                strptime(CAST(fecha AS VARCHAR), '%Y%m%d')::DATE,
                zona_pernoctacion,
                edad,
                sexo,
                numero_viajes,
                CAST(personas AS DOUBLE)
            FROM bronze_mitma_people_day_gau
        ),
        filtered AS (
            SELECT *
            FROM base
            WHERE date IS NOT NULL
              AND overnight_zone IS NOT NULL
              AND age IS NOT NULL
              AND sex IS NOT NULL
              AND n_trips IS NOT NULL
              AND people IS NOT NULL
        )
        SELECT * FROM filtered;
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_people_day").fetchdf()
    print(f"[TASK] Created silver_people_day with {count.iloc[0]['count']:,} records")

    print(con.execute("SELECT * FROM silver_people_day LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_people_day"
    }

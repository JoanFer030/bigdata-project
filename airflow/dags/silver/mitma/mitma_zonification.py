"""
Airflow task for loading MITMA zonification data into Silver layer.
Handles zoning (zonificación) data including geometries, names, and population
for distritos, municipios, and GAU zone types.
"""

import sys
import os
from airflow.sdk import task # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_mitma_zonification():
    """
    Airflow task to transform and standarize zonification data into DuckDB for the specified type.

    Returns:
    - Dict with task status and info
    """
    print("[TASK] Building unified silver zonification table")
    
    con = get_ducklake_connection()

    con.execute("""
        INSTALL spatial;
        LOAD spatial;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE silver_zones AS
        WITH base AS (
            SELECT
                id,
                Nombre AS name,
                'distrito' AS zone_level,
                ST_Multi(ST_GeomFromText(geometry)) AS geometry_obj,
                ST_Centroid(ST_Multi(ST_GeomFromText(geometry))) AS centroid,
                ST_X(ST_Centroid(ST_Multi(ST_GeomFromText(geometry)))) AS centroid_longitude,
                ST_Y(ST_Centroid(ST_Multi(ST_GeomFromText(geometry)))) AS centroid_latitude
            FROM bronze_mitma_distritos

            UNION ALL
            SELECT
                id,
                Nombre,
                'municipio',
                ST_Multi(ST_GeomFromText(geometry)),
                ST_Centroid(ST_Multi(ST_GeomFromText(geometry))),
                ST_X(ST_Centroid(ST_Multi(ST_GeomFromText(geometry)))),
                ST_Y(ST_Centroid(ST_Multi(ST_GeomFromText(geometry))))
            FROM bronze_mitma_municipios

            UNION ALL
            SELECT
                id,
                Nombre,
                'gau',
                ST_Multi(ST_GeomFromText(geometry)),
                ST_Centroid(ST_Multi(ST_GeomFromText(geometry))),
                ST_X(ST_Centroid(ST_Multi(ST_GeomFromText(geometry)))),
                ST_Y(ST_Centroid(ST_Multi(ST_GeomFromText(geometry))))
            FROM bronze_mitma_gau
        )
        SELECT *
        FROM base
        WHERE id IS NOT NULL
          AND name IS NOT NULL
          AND geometry_obj IS NOT NULL;
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_zones").fetchdf()
    print(f"[TASK] Created silver_zones with {count.iloc[0]['count']:,} records")

    print(con.execute("SELECT * FROM silver_zones LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_zones"
    }

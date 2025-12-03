import sys
import os
from airflow.sdk import task # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_distances():
    """
    Airflow task to calculate distances between zones
    and store them in silver_distances table.
    The distances are calculated between zones of the same type.
    """
    print("[TASK] Building silver_distances table")

    con = get_ducklake_connection()

    con.execute("""
        INSTALL spatial;
        LOAD spatial;
    """)

    con.execute("""
        CREATE OR REPLACE TABLE silver_distances AS
        SELECT
            o.id AS origin,
            d.id AS destination,
            ST_Distance_Sphere(o.centroid, d.centroid) / 1000.0 AS distance_km
        FROM silver_zones AS o
            CROSS JOIN silver_zones AS d
        WHERE o.id != d.id
          AND o.zone_level = d.zone_level;
    """)

    count = con.execute("SELECT COUNT(*) AS count FROM silver_distances").fetchdf()
    print(f"[TASK] Created silver_distances with {count.iloc[0]['count']:,} records")

    print(con.execute("SELECT * FROM silver_distances LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(count.iloc[0]['count']),
        "table": "silver_distances"
    }

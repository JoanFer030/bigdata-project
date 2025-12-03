"""
Airflow task for building the unified MITMA OD Silver table.
Includes:
- Type casting
- Weekend / holiday flags
- Null filtering of critical fields
- zone_level standardization
"""

import sys
import os
from airflow.sdk import task  # type: ignore

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils import get_ducklake_connection


@task
def SILVER_mitma_od():
    """
    Creates the unified silver_od table from all Bronze MITMA OD tables.
    Requires that the TEMP table `bronze_spanish_holidays` already exists in the session.

    Returns:
    - Dict with task status and metadata
    """
    print("[TASK] Building silver_od unified OD table")

    con = get_ducklake_connection()

    con.execute("""
        CREATE OR REPLACE TABLE silver_od AS
        WITH base AS (
            -------------------------------------------------------------------
            -- DISTRITOS
            -------------------------------------------------------------------
            SELECT
                'distrito' AS zone_level,
                strptime(CAST(fecha AS VARCHAR) || LPAD(CAST(periodo AS VARCHAR), 2, '0'), '%Y%m%d%H') as datetime,
                origen AS origin_id,
                destino AS destination_id,
                CAST(viajes AS DOUBLE)    AS n_trips,
                CAST(viajes_km AS DOUBLE) AS km_trips,
                distancia AS distance,
                actividad_origen AS origin_activity,
                actividad_destino AS destination_activity,
                residencia AS residence,
                renta AS income,
                edad AS age,
                sexo AS sex,
                CASE WHEN estudio_destino_posible ILIKE 'si' THEN TRUE
                     WHEN estudio_destino_posible ILIKE 'no' THEN FALSE END
                     AS study_possible_destination,
                CASE WHEN estudio_origen_posible ILIKE 'si' THEN TRUE
                     WHEN estudio_origen_posible ILIKE 'no' THEN FALSE END
                     AS study_possible_origin
            FROM bronze_mitma_od_distritos

            UNION ALL

            -------------------------------------------------------------------
            -- MUNICIPIOS
            -------------------------------------------------------------------
            SELECT
                'municipio' AS zone_level,
                strptime(CAST(fecha AS VARCHAR) || LPAD(CAST(periodo AS VARCHAR), 2, '0'), '%Y%m%d%H'),
                origen,
                destino,
                CAST(viajes AS DOUBLE),
                CAST(viajes_km AS DOUBLE),
                distancia,
                actividad_origen,
                actividad_destino,
                residencia,
                renta,
                edad,
                sexo,
                CASE WHEN estudio_destino_posible ILIKE 'si' THEN TRUE
                     WHEN estudio_destino_posible ILIKE 'no' THEN FALSE END,
                CASE WHEN estudio_origen_posible ILIKE 'si' THEN TRUE
                     WHEN estudio_origen_posible ILIKE 'no' THEN FALSE END
            FROM bronze_mitma_od_municipios

            UNION ALL

            -------------------------------------------------------------------
            -- GAU
            -------------------------------------------------------------------
            SELECT
                'gau' AS zone_level,
                strptime(CAST(fecha AS VARCHAR) || LPAD(CAST(periodo AS VARCHAR), 2, '0'), '%Y%m%d%H'),
                origen,
                destino,
                CAST(viajes AS DOUBLE),
                CAST(viajes_km AS DOUBLE),
                distancia,
                actividad_origen,
                actividad_destino,
                residencia,
                renta,
                edad,
                sexo,
                CASE WHEN estudio_destino_posible ILIKE 'si' THEN TRUE
                     WHEN estudio_destino_posible ILIKE 'no' THEN FALSE END,
                CASE WHEN estudio_origen_posible ILIKE 'si' THEN TRUE
                     WHEN estudio_origen_posible ILIKE 'no' THEN FALSE END
            FROM bronze_mitma_od_gau
        ),

        enriched AS (
            SELECT
                *,
                CASE WHEN dayofweek(datetime) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend,
                CASE WHEN date(datetime) IN (SELECT date FROM bronze_spanish_holidays)
                     THEN TRUE ELSE FALSE END AS is_holiday
            FROM base
        ),

        filtered AS (
            SELECT *
            FROM enriched
            WHERE 
                datetime IS NOT NULL
                AND origin_id IS NOT NULL
                AND destination_id IS NOT NULL
                AND n_trips IS NOT NULL
                AND km_trips IS NOT NULL
                AND distance IS NOT NULL
        )

        SELECT * FROM filtered;
    """)

    # Count & preview
    df = con.execute("SELECT COUNT(*) AS count FROM silver_od").fetchdf()
    print(f"[TASK] Created silver_od with {df.iloc[0]['count']:,} records")

    print("[TASK] Sample preview:")
    print(con.execute("SELECT * FROM silver_od LIMIT 10").fetchdf())

    return {
        "status": "success",
        "records": int(df.iloc[0]["count"]),
        "table": "silver_od"
    }

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

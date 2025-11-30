from datetime import datetime
from airflow.sdk import dag, task # type: ignore
from utils import connect_datalake
import pandas as pd

# -----------------------------
# Tareas
# -----------------------------
@task
def check_table(table_name: str):
    """
    Conecta a DuckLake y muestra el número de filas y las 10 primeras filas.
    """
    con = connect_datalake()
    try:
        total_rows = con.execute(f"SELECT COUNT(*) AS total FROM {table_name}").fetchone()[0]
        print(f"Table {table_name} has {total_rows} rows.")

        sample_rows = con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchdf()
        print(f"First 10 rows of {table_name}:\n{sample_rows}")

        return {
            "table": table_name,
            "total_rows": total_rows,
            "sample_rows": sample_rows.to_dict(orient="records")
        }
    finally:
        con.close()


# -----------------------------
# DAG
# -----------------------------
@dag(
    dag_id="verify_bronze_tables",
    start_date=datetime(2025, 11, 30),
    schedule=None,
    catchup=False,
    tags=["verification"]
)
def verify_bronze_tables_dag():
    tables_to_check = ["bronze_od_municipios"]

    for table in tables_to_check:
        check_table(table)


verify_bronze_tables_dag()

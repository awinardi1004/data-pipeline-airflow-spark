from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from airflow.hooks.base import BaseHook
import psycopg2
import traceback

def main():
    try:
        # --- Ambil koneksi dari Airflow Connection ---
        pg_conn = BaseHook.get_connection("pg_conn")

        jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
        properties = {
            "user": pg_conn.login,
            "password": pg_conn.password,
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }

        print(f"PostgreSQL target: {pg_conn.host}:{pg_conn.port}/{pg_conn.schema}")

        # --- Buat schema staging jika belum ada ---
        conn = psycopg2.connect(
            dbname=pg_conn.schema,
            user=pg_conn.login,
            password=pg_conn.password,
            host=pg_conn.host,
            port=pg_conn.port
        )
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.commit()
        cur.close()
        conn.close()
        print("Schema 'staging' ready")

        # --- Spark session ---
        spark = (
            SparkSession.builder
            .appName("Load CSV to Postgres (Airflow Connection)")
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
            .getOrCreate()
        )

        # --- Load CSV ---
        csv_path = "/opt/airflow/include/dfTransjakarta180kRows.csv"
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print(f"CSV loaded: {df.count()} records")

        # --- Transformasi kolom ---
        df_transformed = (
            df.withColumn("transID", col("transID").cast("string"))
            .withColumn("payCardID", col("payCardID").cast("long"))
            .withColumn("payCardBirthDate", col("payCardBirthDate").cast("integer"))
            .withColumn("corridorID", col("corridorID").cast("string"))
            .withColumn("direction", col("direction").cast("integer"))
            .withColumn("tapInStops", col("tapInStops").cast("string"))
            .withColumn("tapInStopsLat", col("tapInStopsLat").cast("double"))
            .withColumn("tapInStopsLon", col("tapInStopsLon").cast("double"))
            .withColumn("stopStartSeq", col("stopStartSeq").cast("integer"))
            .withColumn("tapInTime", col("tapInTime").cast("timestamp"))
            .withColumn("tapOutStops", col("tapOutStops").cast("string"))
            .withColumn("tapOutStopsLat", col("tapOutStopsLat").cast("double"))
            .withColumn("tapOutStopsLon", col("tapOutStopsLon").cast("double"))
            .withColumn("stopEndSeq", col("stopEndSeq").cast("integer"))
            .withColumn("tapOutTime", col("tapOutTime").cast("timestamp"))
            .withColumn("payAmount", col("payAmount").cast("double"))
            .withColumn("load_timestamp", current_timestamp())
        )

        print("Transformation complete")

        # --- Write ke PostgreSQL ---
        df_transformed.write.jdbc(
            url=jdbc_url,
            table="staging.stg_transactions",
            mode="append",
            properties=properties
        )

        print("Data successfully written to staging.stg_transactions")

        spark.stop()

    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()

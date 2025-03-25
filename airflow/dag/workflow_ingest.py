# airflow/dags/workflow_ingest.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'workflow_ingest',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def extract_new_data():
    # Replace with your actual data extraction logic
    """
    Extracts new data from a source (e.g. CSV file, API call, etc.)
    and saves it to a temporary Parquet file.

    This function is intended to be replaced with your actual data extraction logic.
    The example code reads a CSV file and writes it to a Parquet file in the "data/temp" directory.
    """
    
    spark = SparkSession.builder.appName("ExtractData").getOrCreate()
    new_data = spark.read.csv("data/raw/new_data.csv", header=True, inferSchema=True) # example
    new_data.write.parquet("data/temp/new_data.parquet", mode="overwrite")
    spark.stop()
    print("Data Extracted")

def transform_and_load_cdc():
    """
    Transforms the extracted data into CDC Type 2 format and loads it into a Delta Lake table.

    This function reads the temporary Parquet file containing the extracted data, applies CDC Type 2 logic to it, and then merges the transformed data with an existing Delta Lake table (if it exists).

    If the Delta table does not exist, the function creates a new one.

    :return: None
    """
    spark = SparkSession.builder.appName("TransformCDC").getOrCreate()
    new_data = spark.read.parquet("data/temp/new_data.parquet")
    target_table = "data/processed/delta/patients_cdc" # example

    # CDC Type 2 Logic
    new_data = new_data.withColumn("valid_from", F.current_timestamp()).withColumn("valid_to", F.lit(None))

    try:
        existing_data = spark.read.format("delta").load(target_table)

        # Merge Logic (Simplified)
        merged_data = existing_data.unionByName(new_data, allowMissingColumns=True) # use unionByName to handle possible schema changes.
        merged_data.write.format("delta").mode("overwrite").save(target_table)

    except Exception as e:
        print(f"Delta table not found, creating new table. Error: {e}")
        new_data.write.format("delta").mode("overwrite").save(target_table)

    print("CDC applied and data loaded.")
    spark.stop()

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_new_data,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load_cdc',
    python_callable=transform_and_load_cdc,
    dag=dag,
)

extract_data_task >> transform_and_load_task
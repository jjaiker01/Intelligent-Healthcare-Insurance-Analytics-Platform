# pipelines/ingestion/ingest_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when

spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

def ingest_csv_to_delta(filepath, tablename):
    """
    Ingests a CSV file into a Delta table.

    This function reads a CSV file from the specified filepath, infers its schema, and writes it to a Delta table.
    The resulting Delta table is saved with the given tablename in the 'data/processed/delta/' directory.

    Args:
        filepath (str): The file path of the CSV file to ingest.
        tablename (str): The name of the Delta table to create.
    """

    df = spark.read.csv(filepath, header=True, inferSchema=True)
    df.write.format("delta").mode("overwrite").save(f"data/processed/delta/{tablename}")

def ingest_json_to_delta(filepath, tablename):
    """
    Ingests a JSON file into a Delta table.

    This function reads a JSON file from the specified filepath and writes it to a Delta table.
    The resulting Delta table is saved with the given tablename in the 'data/processed/delta/' directory.

    Args:
        filepath (str): The file path of the JSON file to ingest.
        tablename (str): The name of the Delta table to create.
    """
    df = spark.read.json(filepath)
    df.write.format("delta").mode("overwrite").save(f"data/processed/delta/{tablename}")

def transform_patient_data():
    """
    Transforms the patients Delta table to a CDC Type 2 format.

    This function reads the patients Delta table, adds valid_from and valid_to columns, and writes the transformed data back as a new Delta table with the "_cdc" suffix.
    The valid_from column is set to the current timestamp, while the valid_to column is set to NULL.
    """
    
    df = spark.read.format("delta").load("data/processed/delta/patients")
    df = df.withColumn("valid_from", current_timestamp()).withColumn("valid_to", lit(None))
    df.write.format("delta").mode("overwrite").save("data/processed/delta/patients_cdc")

def transform_accident_data():
    """
    Transforms the accidents Delta table to a CDC Type 2 format.

    This function reads the accidents Delta table, adds valid_from and valid_to columns, and writes the transformed data back as a new Delta table with the "_cdc" suffix.
    The valid_from column is set to the current timestamp, while the valid_to column is set to NULL.
    """
    df = spark.read.format("delta").load("data/processed/delta/accidents")
    df = df.withColumn("valid_from", current_timestamp()).withColumn("valid_to", lit(None))
    df.write.format("delta").mode("overwrite").save("data/processed/delta/accidents_cdc")

def check_soat_validity():
    """
    Checks the validity of SOAT records by comparing their end dates to the current timestamp.

    This function reads the soats Delta table, adds an "is_valid" column based on the comparison, and writes the transformed data back as a new Delta table with the "_validity" suffix.
    The "is_valid" column is set to True if the end date is greater than or equal to the current timestamp, and False otherwise.
    """
    df = spark.read.format("delta").load("data/processed/delta/soats")
    df = df.withColumn("is_valid", when(col("end_date") >= current_timestamp(), lit(True)).otherwise(lit(False)))
    df.write.format("delta").mode("overwrite").save("data/processed/delta/soats_validity")

if __name__ == "__main__":
    ingest_csv_to_delta("data/raw/csv/patients.csv", "patients")
    ingest_csv_to_delta("data/raw/csv/accidents.csv", "accidents")
    ingest_csv_to_delta("data/raw/csv/soats.csv", "soats")
    transform_patient_data()
    transform_accident_data()
    check_soat_validity()
    print("Ingestion and transformation complete.")

# Test
def test_ingestion_transformation():
    """
    Tests the ingestion process by reading a CSV file into a Delta table.

    This test ingests the patients.csv file, reads the resulting Delta table, and asserts that the count is greater than 0.
    If the test passes, it prints "Ingestion Test Passed".
    """
    ingest_csv_to_delta("data/raw/csv/patients.csv", "patients_test")
    patients_df = spark.read.format("delta").load("data/processed/delta/patients_test")
    assert patients_df.count() > 0
    print("Ingestion Test Passed")

test_ingestion_transformation()
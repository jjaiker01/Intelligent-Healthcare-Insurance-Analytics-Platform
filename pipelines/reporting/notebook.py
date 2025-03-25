# notebook.ipynb
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format
import os

spark = SparkSession.builder.appName("DailySQLScripts").getOrCreate()

def execute_sql_file(file_path):
    """
    Execute a SQL file and print the result.

    Args:
        file_path (str): Path to the SQL file to execute.

    Returns:
        None

    Raises:
        Exception: If there's an error executing the SQL query.
    """
    try:
        with open(file_path, 'r') as file:
            sql_query = file.read()
        result_df = spark.sql(sql_query)
        # You can add logic here to save the results if needed.
        # Example: result_df.write.parquet(f"results/{os.path.basename(file_path).replace('.sql', '.parquet')}")
        print(f"Executed {file_path} successfully.")
    except Exception as e:
        print(f"Error executing {file_path}: {e}")

# List of SQL files to execute
sql_files = [
    "sql/queries/accident_frequency_by_location_time.sql",
    "sql/queries/accident_severity_analysis.sql",
    "sql/queries/soat_claim_analysis.sql",
    "sql/queries/payment_tracking.sql",
    "sql/queries/patient_medical_history_impact.sql",
    "sql/queries/vehicle_accident_correlation.sql",
    "sql/queries/driver_accident_risk.sql",
    "sql/queries/company_erp_costs.sql",
    "sql/queries/runt_validation_status.sql",
    "sql/queries/ftp_log_analysis.sql",
    "sql/queries/avg_payment_by_severity.sql",
    "sql/queries/accident_count_per_patient.sql",
    "sql/queries/top_10_accident_locations.sql",
    "sql/queries/soat_validity_rate.sql",
    "sql/queries/accident_count_by_year.sql",
    "sql/queries/most_common_accident_causes.sql",
    "sql/queries/driver_average_age_at_accident.sql"
]

# Execute each SQL file
for sql_file in sql_files:
    execute_sql_file(sql_file)

print("Daily SQL scripts execution completed.")

spark.stop()
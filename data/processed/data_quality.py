# healthcare-insurance-analysis/data/processed/data_quality.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull, lit

spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

def check_duplicates(df, primary_key):
    """
    Checks for duplicate records in a DataFrame based on a given primary key.

    Args:
        df (DataFrame): The DataFrame to check for duplicates.
        primary_key (str): The column name(s) to use as the primary key.

    Returns:
        int: The number of duplicate records found.
    """
    
    duplicate_count = df.groupBy(primary_key).agg(count("*").alias("count")).filter(col("count") > 1).count()
    return duplicate_count

def check_nulls(df, columns):
    """
    Checks for null values in a DataFrame based on a given list of columns.

    Args:
        df (DataFrame): The DataFrame to check for null values.
        columns (list): The list of column names to check.

    Returns:
        dict: A dictionary with the null counts for each column, keyed by the column name.
    """
    null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in columns]).collect()[0].asDict()
    return null_counts

def check_outliers(df, column, lower_bound, upper_bound):
    """
    Checks for outliers in a DataFrame column based on specified bounds.

    This function counts the number of records in a DataFrame where the specified column's
    value falls outside the given lower and upper bounds.

    Args:
        df (DataFrame): The DataFrame to check for outliers.
        column (str): The name of the column to evaluate for outliers.
        lower_bound (numeric): The lower threshold for the column values.
        upper_bound (numeric): The upper threshold for the column values.

    Returns:
        int: The number of outlier records found.
    """

    outlier_count = df.filter((col(column) < lower_bound) | (col(column) > upper_bound)).count()
    return outlier_count

def check_foreign_keys(df, foreign_key, reference_table, reference_key):
    """
    Checks for invalid foreign keys in a DataFrame column.

    This function counts the number of records in a DataFrame where the specified foreign
    key column does not have a matching value in the specified reference table.

    Args:
        df (DataFrame): The DataFrame to check for invalid foreign keys.
        foreign_key (str): The name of the foreign key column in the DataFrame.
        reference_table (DataFrame): The reference table to check against.
        reference_key (str): The name of the key column in the reference table.

    Returns:
        int: The number of invalid foreign key records found.
    """
    invalid_fk_count = df.join(reference_table, df[foreign_key] == reference_table[reference_key], "left_anti").count()
    return invalid_fk_count

def check_valid_range(df, column, lower_bound, upper_bound):
    """
    Checks for values in a DataFrame column that fall outside a specified range.

    This function counts the number of records in a DataFrame where the specified column's
    value falls outside the given lower and upper bounds.

    Args:
        df (DataFrame): The DataFrame to check for invalid range values.
        column (str): The name of the column to evaluate for invalid range values.
        lower_bound (numeric): The lower threshold for the column values.
        upper_bound (numeric): The upper threshold for the column values.

    Returns:
        int: The number of invalid range records found.
    """

    invalid_range_count = df.filter((col(column) < lower_bound) | (col(column) > upper_bound)).count()
    return invalid_range_count

def check_valid_values(df, column, valid_values):
    """
    Checks for invalid values in a DataFrame column based on a list of valid values.

    This function counts the number of records in a DataFrame where the specified column's
    value does not match any of the values in the list of valid values.

    Args:
        df (DataFrame): The DataFrame to check for invalid values.
        column (str): The name of the column to evaluate for invalid values.
        valid_values (list): The list of valid values to check against.

    Returns:
        int: The number of invalid value records found.
    """
    invalid_values_count = df.filter(~col(column).isin(valid_values)).count()
    return invalid_values_count

def perform_data_quality_checks(df, table_name):
    # Implement data quality checks based on the table name and the rules defined above.
    # Print the results of each check.
    """
    Performs data quality checks on a given DataFrame based on the table name.

    Checks for duplicates, nulls, outliers, invalid foreign keys, and invalid values
    in specific columns.

    Args:
        df (DataFrame): The DataFrame to perform the data quality checks on.
        table_name (str): The name of the table to perform checks for.

    Returns:
        None
    """
    
    if table_name == "Patients":
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'patient_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['name', 'address', 'medical_history'])}")
        print(f"Outliers in {table_name}.age: {check_outliers(df, 'age', 0, 120)}")
        print(f"Invalid age range in {table_name}.age: {check_valid_range(df, 'age', 0, 120)}")
    elif table_name == "Accidents":
        location_df = spark.read.format("delta").load("data/processed/delta/Locations")
        vehicle_df = spark.read.format("delta").load("data/processed/delta/Vehicles")
        patients_df = spark.read.format("delta").load("data/processed/delta/Patients")
        drivers_df = spark.read.format("delta").load("data/processed/delta/Drivers")

        print(f"Duplicates in {table_name}: {check_duplicates(df, 'accident_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['accident_date', 'location_id', 'vehicle_id', 'patient_id', 'driver_id', 'accident_cause', 'severity'])}")
        print(f"Invalid location_id in {table_name}: {check_foreign_keys(df, 'location_id', location_df, 'location_id')}")
        print(f"Invalid vehicle_id in {table_name}: {check_foreign_keys(df, 'vehicle_id', vehicle_df, 'vehicle_id')}")
        print(f"Invalid patient_id in {table_name}: {check_foreign_keys(df, 'patient_id', patients_df, 'patient_id')}")
        print(f"Invalid driver_id in {table_name}: {check_foreign_keys(df, 'driver_id', drivers_df, 'driver_id')}")
    elif table_name == "SOAT":
        vehicle_df = spark.read.format("delta").load("data/processed/delta/Vehicles")
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'soat_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['start_date', 'end_date', 'insurance_provider', 'coverage_details'])}")
        print(f"Invalid vehicle_id in {table_name}: {check_foreign_keys(df, 'vehicle_id', vehicle_df, 'vehicle_id')}")
        print(f"Invalid date range in {table_name}: {df.filter(col('start_date') > col('end_date')).count()}")
    elif table_name == "Payments":
        accidents_df = spark.read.format("delta").load("data/processed/delta/Accidents")
        patients_df = spark.read.format("delta").load("data/processed/delta/Patients")
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'payment_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['accident_id', 'patient_id', 'payment_date', 'payment_amount', 'payment_type', 'payment_description'])}")
        print(f"Invalid accident_id in {table_name}: {check_foreign_keys(df, 'accident_id', accidents_df, 'accident_id')}")
        print(f"Invalid patient_id in {table_name}: {check_foreign_keys(df, 'patient_id', patients_df, 'patient_id')}")
        print(f"Negative payment amounts in {table_name}: {df.filter(col('payment_amount') < 0).count()}")
    elif table_name == "Locations":
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'location_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['city', 'department', 'latitude', 'longitude'])}")
        print(f"Invalid latitude range in {table_name}: {check_valid_range(df, 'latitude', -90, 90)}")
        print(f"Invalid longitude range in {table_name}: {check_valid_range(df, 'longitude', -180, 180)}")
    elif table_name == "Vehicles":
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'vehicle_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['vehicle_type', 'vehicle_model', 'vehicle_make', 'license_plate'])}")
        #add checks for valid vehicle_type and license_plate formats here.
    elif table_name == "Drivers":
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'driver_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['driver_name', 'driver_license', 'driver_age'])}")
        print(f"Outliers in {table_name}.driver_age: {check_outliers(df, 'driver_age', 16, 120)}")
        #add checks for valid driver_license formats here.
    elif table_name == "RUNT_Validation":
        vehicle_df = spark.read.format("delta").load("data/processed/delta/Vehicles")
        drivers_df = spark.read.format("delta").load("data/processed/delta/Drivers")
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'validation_id')}")
        print(f"Nulls in {table_name}: {check_nulls(df, ['vehicle_id', 'driver_id', 'validation_date', 'validation_status', 'validation_details'])}")
        print(f"Invalid vehicle_id in {table_name}: {check_foreign_keys(df, 'vehicle_id', vehicle_df, 'vehicle_id')}")
        print(f"Invalid driver_id in {table_name}: {check_foreign_keys(df, 'driver_id', drivers_df, 'driver_id')}")
        #add checks for valid validation_status values here.
    elif table_name == "FTP_Logs":
        print(f"Duplicates in {table_name}: {check_duplicates(df, 'log_id')}")

if __name__ == "__main__":
    patients_df = spark.read.format("delta").load("data/processed/delta")
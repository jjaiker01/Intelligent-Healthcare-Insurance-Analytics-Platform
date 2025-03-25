# ml/scripts/train_accident_risk_model.py
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import col, when, datediff, to_date, current_date

spark = SparkSession.builder.appName("AccidentRiskModel").getOrCreate()

def prepare_data():
    """
        Prepare data for training the accident risk model.

        This function reads the accidents_cdc and patients_cdc Delta tables, calculates
        driver age and accident frequency, labels accident risk, and performs feature
        engineering.

        Returns:
            DataFrame: a DataFrame with the features and risk labels
    """
    accidents = spark.read.format("delta").load("data/processed/delta/accidents_cdc")
    patients = spark.read.format("delta").load("data/processed/delta/patients_cdc")

    # Calculate driver age and accident frequency
    accidents = accidents.withColumn("accident_date", to_date(col("accident_date")))
    accidents = accidents.join(patients, "patient_id")
    accidents = accidents.withColumn("driver_age", datediff(current_date(), col("accident_date")) / 365)

    # Label accident risk (e.g., high risk if more than 1 accident in 2 years)
    accidents = accidents.groupBy("driver_id").agg({"accident_id": "count"})
    accidents = accidents.withColumn("risk_label", when(col("count(accident_id)") > 1, 1).otherwise(0))

    # Feature engineering
    assembler = VectorAssembler(inputCols=["driver_age"], outputCol="features")
    indexer = StringIndexer(inputCol="driver_id", outputCol="driver_index")

    return accidents.select("features", "risk_label").dropna()

def train_model(data):
    """
        Train a logistic regression model to predict accident risk.

        This function splits the input data into training and testing sets, creates a
        logistic regression model, and performs hyperparameter tuning using cross-validation.

        Args:
            data: a DataFrame with the features and risk labels

        Returns:
            tuple: a tuple with the trained model and the test data
    """

    train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)

    lr = LogisticRegression(labelCol="risk_label")
    pipeline = Pipeline(stages=[lr])

    paramGrid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=paramGrid,
                              evaluator=BinaryClassificationEvaluator(),
                              numFolds=3)

    model = crossval.fit(train_data)
    return model, test_data

def evaluate_model(model, test_data):
    """
        Evaluate the performance of a trained accident risk model.

        This function takes a trained model and test data as input, uses the model to make
        predictions on the test data, and calculates the area under the ROC curve (AUC)
        of the model's performance.

        Args:
            model: a trained accident risk model
            test_data: a DataFrame with the test data

        Returns:
            None
    """

    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
    auc = evaluator.evaluate(predictions)
    print(f"AUC: {auc}")

if __name__ == "__main__":
    data = prepare_data()
    model, test_data = train_model(data)
    evaluate_model(model, test_data)

    model.bestModel.save("ml/models/accident_risk_model")
    print("Model training complete.")

# Test
def test_ml_model():
    """
        Test the accident risk model by training it on the prepared data and checking
        that the number of predictions is greater than 0.

        This function does not take any input and does not return anything. It is
        intended to be used as a unit test for the accident risk model.

        Returns:
            None
    """

    data = prepare_data()
    model, test_data = train_model(data)
    predictions = model.transform(test_data)
    assert predictions.count() > 0
    print("ML test passed")
test_ml_model()
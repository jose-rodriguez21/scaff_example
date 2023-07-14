from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment

class TestApp(TestCase):
    def test_run_experiment(self):
        parameters = {
            "clients_path": "resources/data/input/clients.csv",
            "clients_schema": "resources/schemas/clients_schema.json",
            "contracts_path": "resources/data/input/contracts.csv",
            "contracts_schema": "resources/schemas/contracts_schema.json",
            "products_path": "resources/data/input/products.csv",
            "products_schema": "resources/schemas/products_schema.json",
            "output": "resources/data/output/final_table",
            "output_schema": "resources/schemas/output_schema.json",
            "fdev_customers_path": "resources/data/input/parquet/t_fdev_customers",
            "fdev_customers_schema": "resources/schemas/t_fdev_customers.output.schema",
            "fdev_phones_path": "resources/data/input/parquet/t_fdev_phones",
            "fdev_phones_schema": "resources/schemas/t_fdev_phones.output.schema",
            "output_rule_path": "resources/data/output/table_wick",
            "output_rule_schema": "resources/schemas/t_fdev_customersphones.output.schema"
        }

        experiment = DataprocExperiment()
        experiment.run(**parameters)

        spark = SparkSession.builder.appName("Test_unite").master("local[*]").getOrCreate()

        output_df = spark.read.parquet(parameters["output"])
        output_rule_df = spark.read.parquet(parameters["output_rule_path"])

        self.assertIsInstance(output_df, DataFrame)
        self.assertIsInstance(output_rule_df, DataFrame)

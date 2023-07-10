from typing import Dict
from unittest import TestCase
from unittest.mock import patch

from pyspark.sql import SparkSession, DataFrame

import exampleenginepythonqiyhbwvw.experiment
from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment


class TestApp(TestCase):

    def test_run_experiment(self):
        parameters: Dict = {
            "output": "resources/data/output/final_table",
            "output_rule_path": "resources/data/output/table_wick"
        }

        experiment = DataprocExperiment()

        with patch(
            "exampleenginepythonqiyhbwvw.experiment.DataprocExperiment.run", return_value = None
        ):
            experiment.run(**parameters)


        spark = SparkSession.builder.appName("unittest_job").master("local[*]").getOrCreate()
        out_df = spark.read.parquet(str(parameters["output_rule_path"]))

        self.assertIsInstance(out_df, DataFrame)

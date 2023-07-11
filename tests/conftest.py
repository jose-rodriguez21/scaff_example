"""Execute the pytest initialization.

Remove it under your own responsibility.
"""
import glob
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from dataproc_sdk import DatioPysparkSession

from exampleenginepythonqiyhbwvw.bussineslogic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


@pytest.fixture(scope="session", autouse=True)
def spark_test():
    """Execute the module setup and initialize the SparkSession.

    Warning: if making any modifications, please make sure that this fixture is executed
    at the beginning of the test session, as the DatioPysparkSession will rely on it internally.

    Yields:
        SparkSession: obtained spark session

    Raises:
        FileNotFoundError: if the jar of Dataproc SDK is not found.
    """
    # Get Dataproc SDK jar path
    jars_path = str(Path(sys.prefix) / 'share' / 'sdk' / 'jars' / 'dataproc-sdk-all-*.jar')
    jars_list = glob.glob(jars_path)
    if jars_list:
        sdk_path = jars_list[0]
    else:
        raise FileNotFoundError(f"Dataproc SDK jar not found in {jars_path}, have you installed the requirements?")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("unittest_job") \
        .config("spark.jars", sdk_path) \
        .master("local[*]") \
        .getOrCreate()

    """
    spark = DatioPysparkSession() \
        .getSparkSession().builder \
        .appName("unittest_job") \
        .config("spark.jars", sdk_path) \
        .master("local[*]") \
        .getOrCreate()
    """

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def business_logic():
    yield BusinessLogic()


@pytest.fixture(scope="session")
def init_values():
    yield InitValues()


@pytest.fixture(scope="session")
def clients_df(init_values):
    parameters_client = {
        "clients_path": "resources/data/input/clients.csv",
        "clients_schema": "resources/schemas/clients_schema.json"
    }
    yield init_values.get_input_df(parameters_client, "clients_path", "clients_schema")

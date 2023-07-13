"""Execute the pytest initialization.

Remove it under your own responsibility.
"""
import glob
import sys
from pathlib import Path

import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType

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


@pytest.fixture(scope="session")
def contracts_df(init_values):
    parameters_contracts = {
        "contracts_path": "resources/data/input/contracts.csv",
        "contracts_schema": "resources/schemas/contracts_schema.json"
    }
    yield init_values.get_input_df(parameters_contracts, "contracts_path", "contracts_schema")


@pytest.fixture(scope="session")
def products_df(init_values):
    parameters_products = {
        "products_path": "resources/data/input/products.csv",
        "products_schema": "resources/schemas/products_schema.json"
    }
    yield init_values.get_input_df(parameters_products, "products_path", "products_schema")


@pytest.fixture(scope="session")
def clients_dummy_df(spark_test):
    data = [Row("111"), Row("111"), Row("111"), Row("111"), Row("123"), Row("123")]
    schema = StructType([
        StructField("cod_client", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def contracts_dummy_df(spark_test):
    data = [Row("111", "aaa"), Row("123", "bbb")]
    schema = StructType([
        StructField("cod_titular", StringType()),
        StructField("cod_producto", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def products_dummy_df(spark_test):
    data = [Row("aaa"), Row("bbb")]
    schema = StructType([
        StructField("cod_producto", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def customers_dummy_df(spark_test):
    data = [Row("2020-03-01", "1111111111111112", "AABBCC11", "1122334455667AA"),
            Row("2020-03-02", "1111111111111113", "AABBCC12", "1122334455667AB"),
            Row("2020-03-03", "1111111111111111112", "AABBCC13", "1122334455667AC"),
            Row("2020-03-04", "1111111111111114", "AABBCC21", "1122334455667AD"),
            Row("2020-03-05", "1111111111111111113", "AABBCC31", "1122334455667AE"),
            Row("2020-03-06", "1111111111111115", "AABBCC33", "1122334455667AF")]
    schema = StructType([
        StructField("gl_date", StringType()),
        StructField("credit_card_number", StringType()),
        StructField("customer_id", StringType()),
        StructField("delivery_id", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def phones_dummy_df(spark_test):
    data = [Row("AABBCC11", "1122334455667AA", "Yes", 7200.00),
            Row("AABBCC12", "1122334455667AB", "Yes", 7800.50),
            Row("AABBCC13", "1122334455667AC", "No", 15000.70),
            Row("AABBCC31", "1122334455667AE", "Yes", 9000.70)]
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("delivery_id", StringType()),
        StructField("prime", StringType()),
        StructField("price_product", DoubleType())
    ])
    yield spark_test.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def customers_phones_dummy_df(spark_test):
    data = [Row("2020-03-01", "1111111111111112", "AABBCC11", "1122334455667AA", "Yes", 7200.00, 34, "Apple"),
            Row("2020-03-02", "1111111111111113", "AABBCC12", "1122334455667AB", "Yes", 7800.50, 36, "Apple"),
            Row("2020-03-03", "1111111111111111112", "AABBCC13", "1122334455667AC", "No", 15000.70, 67, "Sony"),
            Row("2020-03-05", "1111111111111111113", "AABBCC31", "1122334455667AE", "Yes", 9000.70, 12, "XOLO")]
    schema = StructType([
        StructField("gl_date", StringType()),
        StructField("credit_card_number", StringType()),
        StructField("customer_id", StringType()),
        StructField("delivery_id", StringType()),
        StructField("prime", StringType()),
        StructField("price_product", DoubleType()),
        StructField("stock_number", IntegerType()),
        StructField("brand", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)

@pytest.fixture(scope="session")
def customers_phones_v2_dummy_df(spark_test):
    data = [Row("2020-03-01", "1111111111111112", "AABBCC11", "1122334455667AA", "Yes", 7200.00, 34, "Apple", 720.00, 200.00, 56.43),
            Row("2020-03-02", "1111111111111113", "AABBCC12", "1122334455667AB", "Yes", 7800.50, 36, "Apple", 0.0, 140.00, 16.40),
            Row("2020-03-03", "1111111111111111112", "AABBCC13", "1122334455667AC", "No", 15000.70, 67, "Sony", 0.0, 86.00, 84.00),
            Row("2020-03-05", "1111111111111111113", "AABBCC31", "1122334455667AE", "Yes", 9000.70, 12, "XOLO", 0.0, 320.00, 100.03)]
    schema = StructType([
        StructField("gl_date", StringType()),
        StructField("credit_card_number", StringType()),
        StructField("customer_id", StringType()),
        StructField("delivery_id", StringType()),
        StructField("prime", StringType()),
        StructField("price_product", DoubleType()),
        StructField("stock_number", IntegerType()),
        StructField("brand", StringType()),
        StructField("extra_discount", DoubleType()),
        StructField("taxes", DoubleType()),
        StructField("discount_amount", DoubleType())
    ])
    yield spark_test.createDataFrame(data, schema)

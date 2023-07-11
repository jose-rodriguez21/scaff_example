import pytest
from dataproc_sdk import DatioSchema
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.io.init_values import InitValues, InitValuesPyC
import exampleenginepythonqiyhbwvw.common.constants as c


def test_initialize_inputs():
    parameters = {
        "clients_path": "resources/data/input/clients.csv",
        "clients_schema": "resources/schemas/clients_schema.json",
        "contracts_path": "resources/data/input/contracts.csv",
        "contracts_schema": "resources/schemas/contracts_schema.json",
        "products_path": "resources/data/input/products.csv",
        "products_schema": "resources/schemas/products_schema.json",
        "output": "resources/data/output/final_table",
        "output_schema": "resources/schemas/output_schema.json"
    }

    init_values = InitValues()
    clients_df, contracts_df, products_df, output_path, output_schema = init_values.initialize_inputs(parameters)

    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame
    assert type(output_path) == str
    assert type(output_schema) == DatioSchema

def test_get_input_df():
    parameters = {
        "clients_path": "resources/data/input/clients.csv",
        "clients_schema": "resources/schemas/clients_schema.json"
    }

    init_values = InitValues()
    clients_df = init_values.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)

    assert type(clients_df) == DataFrame


def test_get_config_by_name():
    parameters = {
        "clients_path": "resources/data/input/clients.csv",
        "clients_schema": "resources/schemas/clients_schema.json"
    }

    init_values = InitValues()
    io_path, io_schema = init_values.get_config_by_name(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)

    assert type(io_path) == str
    assert type(io_schema) == DatioSchema



def test_initialize_inputs_pyc():
    parameters = {
        "fdev_customers_path": "resources/data/input/parquet/t_fdev_customers",
        "fdev_customers_schema": "resources/schemas/t_fdev_customers.output.schema",
        "fdev_phones_path": "resources/data/input/parquet/t_fdev_phones",
        "fdev_phones_schema": "resources/schemas/t_fdev_phones.output.schema",
        "output_rule_path": "resources/data/output/table_wick",
        "output_rule_schema": "resources/schemas/t_fdev_customersphones.output.schema"
    }

    init_values_pyc = InitValuesPyC()
    phones_df, customers_df, output_rule_path, output_rule_schema = init_values_pyc.initialize_inputs(parameters)

    assert type(phones_df) == DataFrame
    assert type(customers_df) == DataFrame
    assert type(output_rule_path) == str
    assert type(output_rule_schema) == DatioSchema

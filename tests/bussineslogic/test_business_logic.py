from unittest import TestCase

import pytest
from pyspark.sql import Window
from pyspark.sql.functions import count, col

import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


class TestBusinessLogic(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self, clients_df, business_logic, contracts_dummy_df, clients_dummy_df, products_dummy_df):
        self.clients_df = clients_df
        self.business_logic = business_logic
        self.clients_dummy_df = clients_dummy_df
        self.contracts_dummy_df = contracts_dummy_df
        self.products_dummy_df = products_dummy_df

    def test_filter_one(self):
        self.clients_filtered_df = self.business_logic.filter_one(self.clients_df)
        self.assertEquals(self.clients_filtered_df.filter(i.edad() < c.THIRTY_NUMBER).count(), 0)
        self.assertEquals(self.clients_filtered_df.filter(i.edad() > c.FIFTY_NUMBER).count(), 0)
        self.assertEquals(self.clients_filtered_df.filter(i.vip() != c.TRUE_VALUE).count(), 0)

    def test_join_tables(self):
        join_df = self.business_logic.join_tables(self.clients_dummy_df, self.contracts_dummy_df, self.products_dummy_df)
        total_expected_columns = len(self.clients_dummy_df.columns) + \
                                 len(self.contracts_dummy_df.columns) + \
                                 len(self.products_dummy_df.columns) - 1
        self.assertEquals(len(join_df.columns), total_expected_columns)

    def test_filter_by_number_of_contracts(self):
        output_df = self.business_logic.filter_by_number_of_contracts(self.clients_dummy_df)
        validation_df = output_df.select(*output_df.columns, count(i.cod_client.name).over(Window.partitionBy(i.cod_client.name)).alias(c.COUNT_COLUMN))\
                 .filter(col(c.COUNT_COLUMN) < c.THREE_NUMBER)\
                 .drop(c.COUNT_COLUMN)
        self.assertEquals(validation_df.count(), 0)
        self.assertEquals(output_df.count(), 4)

    def test_hash_column(self):
        output_df = self.business_logic.hash_column(self.contracts_dummy_df)
        self.assertEquals(len(output_df.columns), len(self.contracts_dummy_df.columns) + 1)
        self.assertIn("hash", output_df.columns)

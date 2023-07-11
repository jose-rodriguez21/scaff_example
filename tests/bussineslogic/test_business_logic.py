from unittest import TestCase

import pytest
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


class TestBusinessLogic(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self, clients_df, business_logic):
        self.clients_df = clients_df
        self.business_logic = business_logic

    def test_filter_one(self):
        output_df = self.business_logic.filter_one(self.clients_df)
        self.assertEquals(output_df.filter(i.edad() < c.THIRTY_NUMBER).count(), 0)
        self.assertEquals(output_df.filter(i.edad() > c.FIFTY_NUMBER).count(), 0)
        self.assertEquals(output_df.filter(i.vip() != c.TRUE_VALUE).count(), 0)

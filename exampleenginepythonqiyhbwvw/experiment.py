from typing import Dict

from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, avg, lit

from exampleenginepythonqiyhbwvw.bussineslogic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues, InitValuesPyC

import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


class DataprocExperiment:

    def __init__(self):
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:
        self.__logger.info("Executing experiment")

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = init_values.initialize_inputs(parameters)

        logic = BusinessLogic()

        clients_df.show()
        contracts_df.show()
        products_df.show()

        filtered_clients_df: DataFrame = logic.filter_one(clients_df)
        joined_df: DataFrame = logic.join_tables(filtered_clients_df, contracts_df, products_df)
        filtered_by_contracts: DataFrame = logic.filter_by_number_of_contracts(joined_df)
        hashed_df: DataFrame = logic.hash_column(filtered_by_contracts)
        # hashed_df.show(20, False)
        final_df: DataFrame = hashed_df\
            .withColumn(c.HASH_COLUMN, when(i.activo() == c.FALSE_VALUE, lit(0)).otherwise(col(c.HASH_COLUMN)))\
            .filter(col(c.HASH_COLUMN) == "0")
        # final_df.show(20, False)

        self.__datio_pyspark_session.write()\
                                    .mode(c.OVERWRITE)\
                                    .option(c.MODE, c.DYNAMIC)\
                                    .partition_by([i.cod_producto.name, i.activo.name])\
                                    .datio_schema(output_schema)\
                                    .parquet(final_df, output_path)


        """
        Lectura de tablas parquet
        """

        init_values_pc = InitValuesPyC()
        phones_df, customers_df, output_rule_path, output_rule_schema = init_values_pc.initialize_inputs(parameters)

        print("Número de registros:")
        print(customers_df.count())
        print(phones_df.count())

        rule_one_df: DataFrame = logic.rule_one(phones_df)
        print(f"Regla 1:", rule_one_df.count())

        rule_two_df: DataFrame = logic.rule_two(customers_df)
        print(f"Regla 2:", rule_two_df.count())

        rule_three_df: DataFrame = logic.rule_three(rule_two_df, rule_one_df)
        print(f"Regla 3:", rule_three_df.count())

        rule_four_df: DataFrame = logic.rule_four(rule_three_df)
        print(f"Regla 4:", rule_four_df.filter(o.customer_vip() == "Yes").count())

        rule_five_df: DataFrame = logic.rule_five(rule_four_df)
        print(f"Regla 5:", rule_five_df.filter(o.extra_discount() > 0.00).count())

        rule_six_df: DataFrame = logic.rule_six(rule_five_df)
        print(f"Regla 6:", rule_six_df.select(avg(o.final_price.name)).collect())

        rule_seven_df: DataFrame = logic.rule_seven(rule_six_df)
        print(f"Regla 7:", rule_seven_df.count())

        rule_eight_df: DataFrame = rule_seven_df.filter(i.nfc() == "null")
        print(f"Regla 8:", rule_eight_df.count())

        rule_nine_df: DataFrame = logic.rule_nine(rule_seven_df, str(parameters["jwk_date"]))
        print("Regla 9:")
        rule_nine_df.show()

        rule_ten_df: DataFrame = logic.rule_ten(rule_nine_df)
        rule_ten_df_validado: DataFrame = logic.validation_schema_method(rule_ten_df)
        rule_ten_df_validado.show()

        self.__datio_pyspark_session.write() \
                                    .mode(c.OVERWRITE) \
                                    .option(c.MODE, c.DYNAMIC) \
                                    .partition_by([o.jwk_date.name]) \
                                    .datio_schema(output_rule_schema) \
                                    .parquet(rule_ten_df_validado, output_rule_path)

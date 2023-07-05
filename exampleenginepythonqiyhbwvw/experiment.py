from typing import Dict

from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, avg, lit

from exampleenginepythonqiyhbwvw.businesslogic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


class DataprocExperiment:

    def __init__(self):
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:
        self.__logger.info("Executing experiment")

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = init_values.initialize_inputs(parameters)

        """
        clients_df.show()
        contracts_df.show()
        products_df.show()
        """

        logic = BusinessLogic()

        filtered_clients_df: DataFrame = logic.filter_one(clients_df)
        # filtered_clients_df.show()

        joined_df: DataFrame = logic.join_tables(filtered_clients_df, contracts_df, products_df)
        # joined_df.show()

        filtered_by_contracts: DataFrame = logic.filter_by_number_of_contracts(joined_df)
        # filtered_by_contracts.show()

        hashed_df: DataFrame = logic.hash_column(filtered_by_contracts)
        # hashed_df.show(20, False)

        # hashed_df.write.mode("overwrite").partitionBy("cod_producto", "activo").parquet(str(parameters["output"]))

        """
        self.__spark.read\
                    .option("basePath", "resources/data/output/final_table")\
                    .parquet("resources/data/output/final_table/cod_producto=600/activo=false")\
                    .show()
        """

        final_df: DataFrame = hashed_df\
            .withColumn("hash", when(col("activo") == "false", lit(0)).otherwise(col("hash")))\
            .filter(col("hash") == "0")

        # final_df.show(20, False)

        self.__datio_pyspark_session.write()\
                                    .mode("overwrite")\
                                    .option("partitionOverwriteMode", "dynamic")\
                                    .partition_by(["cod_producto", "activo"])\
                                    .datio_schema(output_schema)\
                                    .parquet(final_df, output_path)




        """
        Lectura de tablas parquet

        customers_df = self.read_parquet("fdev_customers", parameters)
        phones_df = self.read_parquet("fdev_phones", parameters)

        print("Número de registros:")
        print(customers_df.count())
        print(phones_df.count())

        rule_one_df: DataFrame = logic.rule_one(phones_df)
        print(f"Regla 1:", rule_one_df.count())

        rule_two_df: DataFrame = logic.rule_two(customers_df)
        print(f"Regla 2:", rule_two_df.count())

        rule_three_df: DataFrame = logic.rule_three(rule_two_df, rule_one_df)
        print(f"Regla 3:", rule_three_df.count())

        rule_four_df: DataFrame = logic.rule_four(rule_three_df).filter(col("customer_vip") == "Yes")
        print(f"Regla 4:", rule_four_df.count())

        rule_five_df: DataFrame = logic.rule_five(rule_three_df)
        print(f"Regla 5:", rule_five_df.filter(col("discount_extra") > 0.00).count())

        rule_six_df: DataFrame = logic.rule_six(rule_five_df)
        print(f"Regla 6:", rule_six_df.select(avg("final_price")).collect())

        rule_seven_df: DataFrame = logic.rule_seven(rule_six_df)
        print(f"Regla 7:", rule_seven_df.count())

        rule_eight_df: DataFrame = rule_seven_df.filter(col("nfc") == "null")
        print(f"Regla 8:", rule_eight_df.count())

        rule_nine_df: DataFrame = logic.rule_nine(rule_seven_df, str(parameters["jwk_date"]))
        print("Regla 9:")
        rule_nine_df.show()

        rule_ten_df: DataFrame = logic.rule_ten(rule_nine_df)
        rule_ten_df.show(20, False)

        rule_ten_df.write\
                   .mode("overwrite")\
                   .partitionBy("jwk_date")\
                   .option("partitionOverwriteMode", "dynamic")\
                   .parquet(str(parameters["output_rule"]))

        """

    def read_parquet(self, table_id, parameters):
        return self.__spark.read\
            .option("header", "true")\
            .parquet(str(parameters[table_id]))

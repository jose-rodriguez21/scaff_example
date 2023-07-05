import datetime

from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, count, sha2, concat_ws, length, when, round, row_number, current_date, datediff


class BusinessLogic:
    def __init__(self):
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_one(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado por Edad y Vip")
        return df.filter((col("edad") >= 30) & (col("edad") <= 50) & (col("vip") == "true"))

    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Aplicando Join")
        return clients_df.join(contracts_df, col("cod_client") == col("cod_titular"), "inner")\
                            .join(products_df, ["cod_producto"], "inner")

    def filter_by_number_of_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrando por el numero de contratos")
        return df.select(*df.columns, count("cod_client").over(Window.partitionBy("cod_client")).alias("count"))\
                    .filter(col("count") > 3)\
                    .drop("count")

    def hash_column(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generando Hash Column")
        return df.select(*df.columns, sha2(concat_ws("||", *df.columns), 256).alias("hash"))

    """
    Funciones formulario 5
    """

    def rule_one(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado entre fechas 2020-03-01 y 2020-03-04,")
        self.__logger.info("descartanto marcas: Dell, Coolpad, Chea, BQ, BLU y codigo: CH, IT, CZ, DK")

        listDates = ["2020-03-01", "2020-03-02", "2020-03-03", "2020-03-04"]
        listNotMarcas = ["Dell", "Coolpad", "Chea", "BQ", "BLU"]
        listNotCodeCountry = ["CH", "IT", "CZ", "DK"]

        return df.filter(col("cutoff_date").isin(listDates))\
                    .filter(~col("brand").isin(listNotMarcas))\
                    .filter(~col("country_code").isin(listNotCodeCountry))

    def rule_two(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado entre fechas 2020-03-01 y 2020-03-04,")
        self.__logger.info("y la longitud del numero de tarjeta sea menor a 17")

        listDates = ["2020-03-01", "2020-03-02", "2020-03-03", "2020-03-04"]

        return df.filter(col("gl_date").isin(listDates)).filter(length("credit_card_number") < 17)


    def rule_three(self, customers_df:DataFrame, phones_df: DataFrame) -> DataFrame:
        self.__logger.info("Inner Join entre las tablas Customers y Phones")
        return customers_df.join(phones_df, ["customer_id", "delivery_id"], "inner")

    def rule_four(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado por clientes vip")
        return df.select(*df.columns, ((col("prime") == "Yes") & (col("price_product") > 7500.00)).alias("customer_vip"))\
                 .select(*df.columns, when(col("customer_vip") == "true", "Yes")\
                                     .when(col("customer_vip") == "false", "No")\
                                     .alias("customer_vip"))

    def rule_five(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Descuento extra")
        listNotMarcas = ["XOLO", "Siemens", "Panasonic", "BlackBerry"]
        return df.select(*df.columns, when((col("prime") == "Yes") & (col("stock_number") < 35), round(col("price_product")*0.1, 2))
                                     .when((col("prime") == "No") & (col("stock_number") >= 35), col("price_product")*0.0)
                                     .otherwise(0.0)
                                     .alias("discount_extra")) \
                 .filter(~col("brand").isin(listNotMarcas))

    def rule_six(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculadon el precio final")
        return df.select(*df.columns, round(col("price_product")+col("taxes")-col("discount_amount")-col("discount_extra"), 2).alias("final_price"))

    def rule_seven(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Top 50")
        return df.select(*df.columns, row_number().over(Window.partitionBy("brand").orderBy(col("final_price").desc())).alias("top"))\
                 .filter(col("top") <= 50)

    def rule_nine(self, df: DataFrame, date) -> DataFrame:
        self.__logger.info("Agregando fecha")
        return df.select(*df.columns, col("gl_date").alias("jwk_date"))\
                 .withColumn("jwk_date", when(col("jwk_date") != "null", date).otherwise(date))

    """
    Funciones formulario 5
    """

    def rule_ten(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calcular la edad de los clientes")
        return df.select(*df.columns, current_date().alias("hoy"))\
                 .select(*df.columns, (datediff(col("hoy"), col("birth_date"))/365).alias("age"))\
                 .withColumn("age", round(col("age")).cast("integer"))

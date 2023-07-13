from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, count, sha2, concat_ws, length, when, round, row_number, current_date, datediff
from pyspark.sql.types import DecimalType, DateType
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


class BusinessLogic:
    def __init__(self):
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_one(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado por Edad y Vip")
        return df.filter((i.edad() >= c.THIRTY_NUMBER) &
                         (i.edad() <= c.FIFTY_NUMBER) &
                         (i.vip() == c.TRUE_VALUE))

    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Aplicando Join")
        return clients_df.join(contracts_df, i.cod_client() == i.cod_titular(), c.INNER_JOIN_TYPE)\
                         .join(products_df, [i.cod_producto.name], c.INNER_JOIN_TYPE)

    def filter_by_number_of_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrando por el numero de contratos")
        return df.select(*df.columns, count(i.cod_client.name).over(Window.partitionBy(i.cod_client.name)).alias(c.COUNT_COLUMN))\
                    .filter(col(c.COUNT_COLUMN) > c.THREE_NUMBER)\
                    .drop(c.COUNT_COLUMN)

    def hash_column(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generando Hash Column")
        return df.select(*df.columns, sha2(concat_ws(c.CONCAT_SEPARATOR, *df.columns), 256).alias(c.HASH_COLUMN))

    """
    Funciones formulario 5
    """

    def rule_one(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado entre fechas 2020-03-01 y 2020-03-04,")
        self.__logger.info("descartanto marcas: Dell, Coolpad, Chea, BQ, BLU y codigo: CH, IT, CZ, DK")

        listDates = [c.MARZO_01, c.MARZO_02, c.MARZO_03, c.MARZO_04]
        listNotMarcas = [c.DELL, c.COOLPAD, c.CHEA, c.BQ, c.BLU]
        listNotCodeCountry = [c.CODE_CH, c.CODE_IT, c.CODE_CZ, c.CODE_DK]

        return df.filter(i.cutoff_date().isin(listDates))\
                 .filter(~i.brand().isin(listNotMarcas))\
                 .filter(~i.country_code().isin(listNotCodeCountry))

    def rule_two(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado entre fechas 2020-03-01 y 2020-03-04,")
        self.__logger.info("y la longitud del numero de tarjeta sea menor a 17")

        listDates = [c.MARZO_01, c.MARZO_02, c.MARZO_03, c.MARZO_04]

        return df.filter(i.gl_date().isin(listDates))\
                 .filter(length(i.credit_card_number.name) < c.SEVENTEEN_NUMBER)


    def rule_three(self, customers_df:DataFrame, phones_df: DataFrame) -> DataFrame:
        self.__logger.info("Inner Join entre las tablas Customers y Phones")
        return customers_df.join(phones_df, [i.customer_id.name, i.delivery_id.name], c.INNER_JOIN_TYPE)

    def rule_four(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtrado por clientes vip")
        return df.select(*df.columns, ((i.prime() == c.YES_VALUE) &
                                       (i.price_product() > c.FILTER_PRICE_NUMBER)).alias(o.customer_vip.name))\
                 .select(*df.columns, when(o.customer_vip() == c.TRUE_VALUE, c.YES_VALUE)\
                                     .when(o.customer_vip() == c.FALSE_VALUE, c.NO_VALUE)\
                                     .alias(o.customer_vip.name))

    def rule_five(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Descuento extra")

        listNotMarcas = ["XOLO", "Siemens", "Panasonic", "BlackBerry"]

        return df.select(*df.columns, when((i.prime() == c.YES_VALUE) & (i.stock_number() < c.THIRTY_FIVE_NUMBER), round(i.price_product()*0.1, 2))
                                     .when((i.prime() == c.NO_VALUE) & (i.stock_number() >= c.THIRTY_FIVE_NUMBER), i.price_product()*0.0)
                                     .otherwise(0.0)
                                     .alias(o.extra_discount.name)) \
                                     .filter(~i.brand().isin(listNotMarcas))

    def rule_six(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculadon el precio final")
        return df.select(*df.columns, round(i.price_product()+i.taxes()-i.discount_amount()-o.extra_discount(), 2)
                                                                                            .alias(o.final_price.name))

    def rule_seven(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Top 50")
        return df.select(*df.columns, row_number().over(Window.partitionBy(i.brand.name).orderBy(o.final_price().desc())).alias(o.brands_top.name))\
                 .filter(o.brands_top() <= c.FIFTY_NUMBER)

    def rule_nine(self, df: DataFrame, date) -> DataFrame:
        self.__logger.info("Agregando fecha")
        return df.select(*df.columns, i.gl_date().alias(o.jwk_date.name))\
                 .withColumn(o.jwk_date.name, when(o.jwk_date() != "null", date).otherwise(date))\
                 .withColumn(o.jwk_date.name, o.jwk_date().cast(DateType()))

    """
    Funciones formulario 5
    """

    def rule_ten(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calcular la edad de los clientes")
        return df.select(*df.columns, current_date().alias("hoy"))\
                 .select(*df.columns, (datediff(col("hoy"), i.birth_date())/365).cast("integer").alias(o.age.name)) \
                 .select(i.city_name(),
                         i.street_name(),
                         i.credit_card_number(),
                         i.last_name(),
                         i.first_name(),
                         o.age(),
                         i.brand(),
                         i.model(),
                         i.nfc(),
                         i.country_code(),
                         i.prime(),
                         o.customer_vip(),
                         i.taxes(),
                         i.price_product(),
                         i.discount_amount(),
                         o.extra_discount(),
                         o.final_price(),
                         o.brands_top(),
                         o.jwk_date())

    def validation_schema_method(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Validar schema del DataFrame")
        return df.withColumn(i.taxes.name, i.taxes().cast(DecimalType(9, 2)))\
                 .withColumn(i.discount_amount.name, i.discount_amount().cast(DecimalType(9, 2)))\
                 .withColumn(o.extra_discount.name, o.extra_discount().cast(DecimalType(9, 2)))\
                 .withColumn(o.final_price.name, o.final_price().cast(DecimalType(9, 2)))

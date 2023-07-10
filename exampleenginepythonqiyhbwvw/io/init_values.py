from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import exampleenginepythonqiyhbwvw.common.constants as c

class InitValues:

    def __init__(self):
        self.__logger = get_user_logger(InitValues.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")

        clients_df = self.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)
        contracts_df = self.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
        products_df = self.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)

        output_path, output_schema = self.get_config_by_name(parameters, c.OUTPUT_PATH, c.OUTPUT_SCHEMA)

        return clients_df, contracts_df, products_df, output_path, output_schema


    def get_config_by_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get config for " + key_path)
        io_path = parameters[key_path]
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        return io_path, io_schema

    def get_input_df(self, parameters, key_path, key_schema):
        self.__logger.info("Reading from " + key_path)
        io_path, io_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read()\
                                           .datioSchema(io_schema) \
                                           .option(c.HEADER, c.TRUE_VALUE) \
                                           .option(c.DELIMITER, c.COMMA) \
                                           .csv(io_path)

class InitValuesPyC:
    def __init__(self):
        self.__logger = get_user_logger(InitValuesPyC.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")

        customers_df = self.get_input_df(parameters, c.CUSTOMERS_PATH, c.CUSTOMERS_SCHEMA)
        phones_df = self.get_input_df(parameters, c.PHONES_PATH, c.PHONES_SCHEMA)

        output_rule_path, output_rule_schema = self.get_config_name(parameters, c.OUTPUT_RULE_PATH, c.OUTPUT_RULE_SCHEMA)

        return phones_df, customers_df, output_rule_path, output_rule_schema


    def get_config_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get config for " + key_path)
        io_path = parameters[key_path]
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        return io_path, io_schema

    def get_input_df(self, parameters, key_path, key_schema):
        self.__logger.info("Reading from " + key_path)
        io_path, io_schema = self.get_config_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read()\
                                           .datioSchema(io_schema) \
                                           .option(c.HEADER, c.TRUE_VALUE) \
                                           .parquet(io_path)

"""Implements the Snowflake destination"""

import logging

from common.context_manager import SafeContextManager, DatabaseConnectionContextManager
from common.main import Main
from common.service_management import Service
from sqlalchemy import MetaData, Table, create_engine

from snowflake.sqlalchemy import URL


class SnowflakeDestination(SafeContextManager, Service):
    """Implements the Snowflake destination"""

    _DEFAULT_MAXIMUM_POOL_SIZE = 15
    _DEFAULT_MINIMUM_POOL_SIZE = 5

    def __init__(self, job_id, destination):
        self._job_id = job_id
        snowflake_destination = destination.snowflake
        self._input_schema_2_output_schema = {}
        for schema_mapping in destination.schemaMapping:
            self._input_schema_2_output_schema[schema_mapping.input] = schema_mapping.output
        catalog_name, schema_name, table_name = SnowflakeDestination.split_table_name(
            destination.fullyQualifiedTableName)
        url = URL(
            account=snowflake_destination.account,
            database=catalog_name,
            password=snowflake_destination.password,            
            role=snowflake_destination.role,            
            schema=schema_name,
            user=snowflake_destination.user,
            warehouse=snowflake_destination.warehouse
        )
        max_overflow = SnowflakeDestination._DEFAULT_MAXIMUM_POOL_SIZE - SnowflakeDestination._DEFAULT_MINIMUM_POOL_SIZE
        connection_argument_2_value = {
            "echo": Main.logger().level == logging.DEBUG,
            "max_overflow": max_overflow,
            "pool_pre_ping": True,
            "pool_size": SnowflakeDestination._DEFAULT_MINIMUM_POOL_SIZE
        }
        self._connection_pool = create_engine(url, **connection_argument_2_value)
        self._table = self._load_table(table_name)

    def _exit(self):
        self.stop()
    
    def _load_table(self, table_name):
        """Loads the table metadata"""
        with DatabaseConnectionContextManager(self._connection_pool) as manager:
            connection = manager.connection()
            metadata = MetaData(bind=connection)
            Table(table_name, metadata, autoload=True, autoload_with=connection)
        return metadata.tables[table_name]
    
    @staticmethod
    def split_table_name(fully_qualified_table_name):
        """Splits the table name"""
        parts = fully_qualified_table_name.split(".")
        length = len(parts)
        catalog_name, schema_name, table_name = None, None, None
        if length > 0:
            table_name = parts[-1]
        if length > 1:
            schema_name = parts[-2]
        if length > 2:
            catalog_name = ".".join(parts[:-2])
        return catalog_name, schema_name, table_name
    
    def load(self, data):
        """Loads the data into the destination"""
        descriptors = data.descriptors
        values = []
        for data_value in data.values:
            value_mapping = {}
            for index, value in enumerate(data_value):
                column_name = self._input_schema_2_output_schema[descriptors[index]].lower()
                value_mapping[column_name] = value
            values.append(value_mapping)
        with DatabaseConnectionContextManager(self._connection_pool) as manager:
            manager.connection().execute(self._table.insert().values(values))

    def stop(self):
        if not self._connection_pool:
            return
        self._connection_pool.dispose()

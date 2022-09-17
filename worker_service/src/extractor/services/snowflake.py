"""Implements the Azure data extractions service"""

import logging

import pandas as pd
from common.context_manager import (
    DatabaseConnectionContextManager, SafeContextManager)
from common.main import Main
from extractor.services.base import AbstractSource
from sqlalchemy import create_engine

from snowflake.sqlalchemy import URL


class SnowflakeSource(SafeContextManager, AbstractSource):
    """Implements the Azure Source"""

    _DATE_FORMAT = "%Y-%m-%d"
    _INNER_ACCOUNT_USAGE_QUERY = """
        select start_time::date as usage_date,
            warehouse_name,
            case when warehouse_name='CLOUD_SERVICES_ONLY' then 'CLOUD SERVICES' else 'COMPUTE' end as service_type,
            sum(credits_used_compute) as total_credits_used_compute,
            sum(credits_used_cloud_services) as total_credits_used_cloud_services,
            sum(credits_used_compute) * 3.7 as total_compute_cost,
            sum(credits_used_cloud_services) * 3.7 as total_cloud_services_cost,
            0 as total_credits_used_storage,
            0 * 46 as total_storage_cost
        from snowflake.account_usage.warehouse_metering_history where usage_date between '%s' and '%s'
        group by 1,2,3
        union all
        select usage_date,
            NULL as warehouse_name,
            'STORAGE' as service_type,
            0 as total_credits_used_compute,
            0 as total_credits_used_cloud_services,
            0 as total_compute_cost,
            0 as total_cloud_services_cost,
            avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as total_credits_used_storage,
            (avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4)) * 46 as total_storage_cost
        from snowflake.account_usage.storage_usage where usage_date between '%s' and '%s'
        group by 1,2,3
    """
    _COUNT_TOTAL_RECORDS_QUERY = "select count(*) as total_records from (%s)"
    _ACCOUNT_USAGE_QUERY = "select * from (%s) order by 1,2,3 limit %d offset %d"
    _DEFAULT_MAXIMUM_POOL_SIZE = 15
    _DEFAULT_MINIMUM_POOL_SIZE = 5

    def __init__(self, job_id, source):
        self._offset = 0
        self._job_id = job_id
        connection = source.snowflake.connection
        self._input_schema_2_output_schema= {}
        for schema_mapping in source.schemaMapping:
            self._input_schema_2_output_schema[schema_mapping.input] = schema_mapping.output
        url = URL(
            account=connection.account,
            database=connection.database,
            password=connection.password,            
            role=connection.role,            
            schema=connection.schema,
            user=connection.user,
            warehouse=connection.warehouse
        )
        max_overflow = SnowflakeSource._DEFAULT_MAXIMUM_POOL_SIZE - SnowflakeSource._DEFAULT_MINIMUM_POOL_SIZE
        connection_argument_2_value = {
            "echo": Main.logger().level == logging.DEBUG,
            "max_overflow": max_overflow,
            "pool_pre_ping": True,
            "pool_size": SnowflakeSource._DEFAULT_MINIMUM_POOL_SIZE
        }
        self._inner_account_usage_query = SnowflakeSource._INNER_ACCOUNT_USAGE_QUERY % (
            source.fromTimestamp, source.toTimestamp, source.fromTimestamp, source.toTimestamp
        )
        self._batch_size = source.batchSize
        self._connection_pool = create_engine(url, **connection_argument_2_value)
    
    def _exit(self):
        self.stop()

    def _extract(self, offset):
        """Extracts the data from the Azure"""
        Main.logger().debug(
            "extracing the data from the snowflake with offset %d of the %s job", offset, self._job_id)
        with DatabaseConnectionContextManager(self._connection_pool) as manager:
            dataframe = pd.read_sql(
                SnowflakeSource._ACCOUNT_USAGE_QUERY % (
                    self._inner_account_usage_query, self._batch_size, offset), manager.connection())
        Main.logger().debug(
            "extracted the data from the snowflake with offset %d of the %s job", offset, self._job_id)            
        self._offset = offset + self._batch_size
        return dataframe, offset
    
    def _transform(self, dataframe, offset):
        """Transforms the data from the Azure"""
        Main.logger().debug(
            "transforming the data from the azure using %s of the %s job", offset, self._job_id)
        dataframe["usage_date"] = dataframe["usage_date"].apply(lambda x: x.strftime(SnowflakeSource._DATE_FORMAT))
        Main.logger().debug(
            "transformed the data from the azure using %s of the %s job", offset, self._job_id)
    
    def extract_and_transform(self, result_handler):        
        """Implements the extraction and performs transformation"""
        total_records = 0
        with DatabaseConnectionContextManager(self._connection_pool) as manager:
            result = manager.connection().execute(
                SnowflakeSource._COUNT_TOTAL_RECORDS_QUERY % self._inner_account_usage_query).fetchone()
            total_records = int(result[0])
        dataframe, offset = self._extract(self._offset)
        self._transform(dataframe, offset)
        descriptors = []
        for key in list(dataframe.keys()):
            descriptors.append(self._input_schema_2_output_schema[key])
        result_handler({
            "descriptors": descriptors,
            "values": dataframe.values.tolist()
        })
        if self._offset >= total_records:
            return
        self.extract_and_transform(result_handler)
    
    def stop(self):
        if not self._connection_pool:
            return
        self._connection_pool.dispose()

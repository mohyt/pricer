"""Implements extractor service manager"""

import json
import time

from common.kafka import KafkaSink, KafkaSource
from common.main import Main
from common.service_management import ServiceManager
from extractor.services.azure import AzureSource
from extractor.services.snowflake import SnowflakeSource


class ExtractorServiceManager(ServiceManager):

    """Implements extractor service manager"""

    _SOURCE_TYPE_2_CLS = {
        "azure": AzureSource,
        "snowflake": SnowflakeSource
    }

    def __init__(self, configuration):
        self._kafka_source_configuration = configuration.kafkaSourceConfiguration
        self._kafka_sink_configuration = configuration.kafkaSinkConfiguration
        self._kafka_source = None
        self._kafka_sink = None
    
    def _on_message_received(self, job):
        job_id = job.jobId
        source = job.source
        source_type = source.type
        Main.logger().info("extracting the data for the '%s' source as part of %s job", source_type, job_id)
        clazz = ExtractorServiceManager._SOURCE_TYPE_2_CLS.get(source_type.lower())
        clazz_instance = clazz(job_id, source)
        clazz_instance.extract_and_transform(lambda data: self._send_data_to_sink(job.destination, job_id, data))
    
    def _send_data_to_sink(self, destination, job_id, data):
        if not data or not data.values:
            return
        producer_data = json.dumps(
            {
                "data": data,                
                "destination": destination.to_dict(),
                "jobId": job_id
            })
        self._kafka_sink.send_message([producer_data])

    def prepare(self):
        """Prepares the service manager"""
        time.sleep(15)
        self._kafka_source = KafkaSource(self._kafka_source_configuration, self._on_message_received)
        self._kafka_sink = KafkaSink(self._kafka_sink_configuration)

    def start(self):
        """Starts the service manager"""
        self._kafka_source.start()


    def stop(self):
        """Stops the service manager"""
        self._kafka_source.stop()

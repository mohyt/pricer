"""Implements loader service manager"""

import time

from common.kafka import KafkaSource
from common.main import Main
from common.service_management import ServiceManager

from loader.services.snowflake import SnowflakeDestination


class LoaderServiceManager(ServiceManager):

    """Implements loader service manager"""

    _DESTINATION_TYPE_2_CLS = {
        "snowflake": SnowflakeDestination
    }

    def __init__(self, configuration):
        self._kafka_source_configuration = configuration.kafkaSourceConfiguration
        self._kafka_source = None
    
    def _on_message_received(self, job):
        job_id = job.jobId
        destination = job.destination
        destination_type = destination.type.lower()
        Main.logger().info("laoding the data to the '%s' destination as part of %s job", destination_type, job_id)
        clazz = LoaderServiceManager._DESTINATION_TYPE_2_CLS.get(destination_type)
        with clazz(job_id, destination) as clazz_instance:
            clazz_instance.load(job.data)
    
    def prepare(self):
        """Prepares the service manager"""
        time.sleep(15)
        self._kafka_source = KafkaSource(self._kafka_source_configuration, self._on_message_received)

    def start(self):
        """Starts the service manager"""
        self._kafka_source.start()

    def stop(self):
        """Stops the service manager"""
        self._kafka_source.stop()

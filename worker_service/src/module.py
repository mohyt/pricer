"""Implements module for extractor service"""

import os

from common.json_loader import RecursiveNamespace
from extractor.manager import ExtractorServiceManager
from health.controller import HealthServiceController
from health.manager import HealthServiceManager
from loader.manager import LoaderServiceManager
from rest_server.module import BaseModule

class WorkerModule(BaseModule):
    """Module class for extractor service"""

    def initialize(self):
        """Initializes the extractor module"""
        running_mode = os.environ["RUNNING_MODE"]
        configuration_as_dict = {
            "kafkaSourceConfiguration": {
                "bootstrapServers": [str(os.environ["SOURCE_BOOTSTRAP_SERVER"])],
                "group": str(os.environ["SOURCE_GROUP"]),
                "topic": str(os.environ["SOURCE_TOPIC"])
            }
        }
        if running_mode == "extractor":
            configuration_as_dict["kafkaSinkConfiguration"] = {
                "bootstrapServers": [str(os.environ["SINK_BOOTSTRAP_SERVER"])],
                "topic": str(os.environ["SINK_TOPIC"])
            }
        configuration = RecursiveNamespace.map_entry(configuration_as_dict)
        self.add_controller(HealthServiceController, HealthServiceManager())
        if running_mode == "extractor":
            self.add_service_manager(ExtractorServiceManager(configuration))
            return
        self.add_service_manager(LoaderServiceManager(configuration))

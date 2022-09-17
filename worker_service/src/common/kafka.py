from threading import Thread

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from common.exception import ExceptionUtilities
from common.json_loader import JsonLoader
from common.main import Main
from common.service_management import Service


class ConsumerThread(Thread):
    """KafkaConsumer Thread"""

    def __init__(self, consumer, message_handler):
        super().__init__()
        self._consumer = consumer
        self._message_handler = message_handler
    
    def run(self):
        while True:
            message = self._consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                if isinstance(message.error(), KafkaError):
                    raise message.error()
                raise KafkaException(message.error())
            try:
                transformed_message = JsonLoader.loads(message.value().decode('utf-8'))                
            except BaseException as ex:
                Main.logger().error(
                    "failed to deserialize the message from the '%s', details: %s", message.topic(),
                    ExceptionUtilities.message(ex))
                continue
            try:
                self._message_handler(transformed_message)
            except BaseException as ex:
                Main.logger().error(
                    "failed to handle the message from the '%s', details: %s", message.topic(),
                    ExceptionUtilities.message(ex))
                continue

class KafkaSource(Service):
    """Implements the Kafka Source"""

    def __init__(self, configuration, message_handler):
        consumer = Consumer({
            "auto.offset.reset": "earliest",
            "bootstrap.servers": ",".join(configuration.bootstrapServers),
            "enable.auto.commit": True,
            "group.id": configuration.group
        })
        consumer.subscribe([configuration.topic])
        self._consumer_thread = ConsumerThread(
            consumer=consumer, message_handler=message_handler)
    
    def start(self):
        """Starts the service"""
        self._consumer_thread.start()

    def stop(self):
        if self._consumer_thread:
            self._consumer_thread.join()

class KafkaSink(Service):
    """Implements the Kafka Sink"""

    def __init__(self, configuration):
        self._topic = configuration.topic
        self._producer = Producer({
            "bootstrap.servers": ",".join(configuration.bootstrapServers)
        })

    @staticmethod
    def _message_ack(error, message):
        """Waits for the message send operations to be acknowledged"""
        if error is not None:
            Main.logger().error(
                "failed to product the %s message, details: %s", message, ExceptionUtilities.message(error))
            return
        Main.logger().debug(
            "delivered the %s message to the %s topic", message.value(), message.topic())
        
    def send_message(self, message_batch):
        """Sends messages to the kafka producer"""
        for message in message_batch:
            self._producer.poll(0)
            Main.logger().debug("producing the %s message to the %s topic", message, self._topic)
            self._producer.produce(self._topic, message, callback=self._message_ack)
        self._producer.flush()

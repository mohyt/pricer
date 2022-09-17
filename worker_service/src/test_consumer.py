from common.kafka import KafkaSource
from common.main import Main

Main.initialize("test_consumer")
def on_message_received(message):
    print(message.__dict__)

c = KafkaSource(["localhost:9092"], "test", on_message_received, ["EXTRACTION_OUTPUT"])
try:
    c.consume()
finally:
    c.close_consumer()
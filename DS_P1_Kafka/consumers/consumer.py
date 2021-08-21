"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer,OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer,CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""
    BROKER_URL = "PLAINTEXT://localhost:9092"
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        AUTO_OFFSET_RESET = 'earliest' if self.offset_earliest else 'latest'
        self.broker_properties = {"bootstrap.servers": KafkaConsumer.BROKER_URL, "group.id": "0","auto.offset.reset": AUTO_OFFSET_RESET}

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            #schema_registry = CachedSchemaRegistryClient({"url": KafkaConsumer.SCHEMA_REGISTRY_URL})
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)
            #pass

        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        #logger.info("on_assign is incomplete - skipping")
        if self.offset_earliest:
            print("OFFSET SETTED",OFFSET_BEGINNING)
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
                
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)
        
            
            
    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                logger.info(f"attempt to consume {self.topic_name_pattern}")
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        message = self.consumer.poll(self.consume_timeout)
        #print(message,message.topic())
        if message is None:
            logger.info("no message received by consumer")
            return 0
        elif message.error() is not None:
            logger.info(f"error from consumer {message.error()}")
            return 0
        else:
            print(f"consumed message {message.key()}")
            self.message_handler(message)
            return 1

        #logger.info("_consume is incomplete - skipping")
        #return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
    
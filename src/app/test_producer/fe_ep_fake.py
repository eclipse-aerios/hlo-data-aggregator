'''
    For emulating HLO-FE-EP communication
    Send protobuf message with service id
    Use it as python module
    $ python3 -m  app.test_producer.fe_ep_fake
'''
from confluent_kafka import Producer, KafkaException
from app.utils.kafka_client import create_fake_feinput, serialize_to_bytes
from app.config import CONSUMER_TOPIC, consumer_config
from app.utils.log import get_app_logger


logger = get_app_logger()

def on_delivery(err, msg):
    '''
     Callback for kafka delivering message
    '''
    if err is not None:
        logger.error('Delivery failed: %s', err)
    else:
        logger.info('Message delivered to %s [%s]', msg.topic(), msg.partition())

def produce_message():
    '''
    Deliver message to redpanda
    '''
    producer = Producer(consumer_config)
    # Create test redpanda message
    # According to protobuf service data model
    # And (binary) serialize it
    protobuf_msg = serialize_to_bytes(create_fake_feinput())

    try:
        # Sending a message to the topic
        producer.produce(topic=CONSUMER_TOPIC, value=protobuf_msg, callback=on_delivery)

        # Wait for delivery confirmation
        producer.poll(5)

    except KafkaException as e:
        logger.error('Kafka Error: %s', e)

    finally:
        # Close the producer
        producer.flush()

if __name__ == "__main__":
    produce_message()

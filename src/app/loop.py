'''
    Component runtime Control
    Subscribe to kafka and wait for events from HLO_FE_EP....
'''
import traceback
from confluent_kafka import Consumer, KafkaException
from app import config
from app.utils.log import get_app_logger
import app.utils.ngsild as ngsild
from app.utils.kafka_client import parse_from_bytes, produce_message

logger = get_app_logger()


def run():
    '''
        Staying on a loop and awaiting redpanda messages
        Messages should be binary formated and modeled according to protobuf models in gitlab:
          https://gitlab.aeriOS-project.eu/wp3/t3.3/specs
    '''
    logger.info('**Data aggregator service started**')
    logger.info("Development mode: %s", config.DEV)
    logger.info("CB URL: %s", config.API_URL)
    logger.info("CB PORT: %s", config.API_PORT)
    # logger.info("REDPANDA BROKER: %s", config.consumer_config['bootstrap.servers'])
    # logger.info("GROUP_ID: %s", config.consumer_config['group.id'])
    # logger.info("OFFSET_RESET: %s", config.consumer_config['auto.offset.reset'])
    logger.info("CONSUMER_TOPIC: %s", config.CONSUMER_TOPIC)
    logger.info("PRODUCER_TOPIC: %s", config.PRODUCER_TOPIC)
    logger.info("TOKEN_URL: %s", config.TOKEN_URL)

    consumer = Consumer(config.consumer_config)
    # Subscribe to the topic
    consumer.subscribe([config.CONSUMER_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                # No message was received before the timeout expired.
                continue
            if msg.error():
                if msg.error().code() == KafkaException:
                    # End of partition event
                    logger.error('%s %s reached end at offset %s\n',
                                 msg.topic(), msg.partition(), {msg.offset()})
                else:
                    logger.error('Error recceiving message: %s', msg.error())
                    raise KafkaException(msg.error())
            else:
                # Process the message
                event_data = msg.value()  #.decode('utf-8')
                logger.info('Message received: %s', event_data)

                #  Parse protobuf msg and get Service Id
                protbuf_msg = parse_from_bytes(event_data)
                logger.info("Deserialize input from RedPanda: %s", protbuf_msg)
                service_id = protbuf_msg.service.id

                # Get pydantic modeled output for allocator
                hlo_allocator_output_py = ngsild.create_hlo_allocator_py(
                    service_id)
                if config.DEV:
                    logger.info(hlo_allocator_output_py)

                # Failed to satisfy requirments of all service components
                if not hlo_allocator_output_py:
                    pass
                else:
                    # translate to protobuf and send to kafka topic
                    produce_message(hlo_allocator_output_py)
                    consumer.commit(asynchronous=True)
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.error(traceback.format_exc(chain=True))
    finally:
        # Close the consumer
        logger.info('Closing kafka consumer')
        consumer.close()
        run()

'''
    Component runtime Control
    Subscribe to kafka and wait for events from HLO_FE_EP....
'''
from confluent_kafka import Consumer, KafkaException
from app import config
from app.utils.log import get_app_logger
import app.services.ngsild as ngsild
from app.clients.kafka_client import parse_from_bytes, produce_message
from app.utils import errors
from app.services.continuum import set_service_failed

logger = get_app_logger()


def run() -> None:
    '''
    Staying on a loop and awaiting redpanda messages
    Messages should be binary formated and modeled according to protobuf models in gitlab:
    https://gitlab.aeros-project.eu/wp3/t3.3/specs
    '''
    logger.info("**Data aggregator service started**")
    logger.info("Development mode: %s", config.DEV)
    logger.info("CB URL: %s", config.API_URL)
    logger.info("CB PORT: %s", config.API_PORT)
    logger.info("CONSUMER_TOPIC: %s", config.CONSUMER_TOPIC)
    logger.info("PRODUCER_TOPIC: %s", config.PRODUCER_TOPIC)
    logger.info("TOKEN_URL: %s", config.TOKEN_URL)

    while True:
        consumer = Consumer(config.consumer_config)
        consumer.subscribe([config.CONSUMER_TOPIC])
        logger.info(
            "Kafka consumer started and subscribed to topic %s",
            config.CONSUMER_TOPIC,
        )

        try:
            _consume_forever(consumer)
        except KeyboardInterrupt:
            logger.info("Kafka consumer interrupted by user")
            break
        except Exception:
            logger.exception(
                "Kafka consumer loop crashed; restarting consumer")
        finally:
            logger.info("Closing kafka consumer")
            consumer.close()


def _consume_forever(consumer: Consumer) -> None:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            _handle_kafka_poll_error(msg)

        _process_message(consumer, msg)


def _handle_kafka_poll_error(msg) -> None:
    err = msg.error()
    logger.error("Kafka consumer error: %s", err)
    raise KafkaException(err)


def _process_message(consumer: Consumer, msg) -> None:
    try:
        _process_message_impl(consumer, msg)
    except Exception:
        logger.exception(
            "Unexpected uncaught failure while processing Kafka message")
        consumer.commit(asynchronous=True)


def _process_message_impl(consumer: Consumer, msg) -> None:
    event_data = msg.value()
    logger.info("Message received: %s", event_data)

    if not event_data:
        logger.warning("Received empty message on Kafka topic, skipping")
        consumer.commit(asynchronous=True)
        return

    protbuf_msg = parse_from_bytes(event_data)
    logger.info("Deserialize input from RedPanda: %s", protbuf_msg)

    service_id = protbuf_msg.service.id

    try:
        hlo_allocator_output_py = ngsild.create_hlo_allocator_py(service_id)
    except errors.aeriosError as ex:
        fail_service_and_commit(
            consumer,
            service_id,
            "Failed to build allocator payload",
            ex,
        )
        return

    if not hlo_allocator_output_py:
        logger.warning(
            "No allocator output produced for service %s; committing message",
            service_id,
        )
        consumer.commit(asynchronous=True)
        return

    if config.DEV:
        logger.info("Allocator payload: %s", hlo_allocator_output_py)

    produce_message(hlo_allocator_output_py)

    consumer.commit(asynchronous=True)


def fail_service_and_commit(
    consumer,
    service_id: str,
    message: str,
    exc: Exception | None = None,
) -> None:
    '''
    a single rollback exit helper to mark the service as failed in CB and commit the message, 
        to avoid reprocessing of messages that cause exceptions
    '''
    if exc is not None:
        logger.error("%s for service %s: %s", message, service_id, exc)
    else:
        logger.error("%s for service %s", message, service_id)

    try:
        set_service_failed(service_id)
    except Exception as rollback_ex:
        logger.exception(
            "Failed to mark service %s as FAILED during rollback: %s",
            service_id,
            rollback_ex,
        )

    consumer.commit(asynchronous=True)

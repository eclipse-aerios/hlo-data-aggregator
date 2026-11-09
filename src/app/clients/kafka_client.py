'''
    Protobuf and kafka related functions
'''
from typing import List, Optional
from threading import Lock
import json
from confluent_kafka import Producer, KafkaException
from app.config import PRODUCER_TOPIC, producer_config, DEV
from app.utils.log import get_app_logger
from app.app_models.protobuf.generated import data_aggregator_pb2 as aggregator
from app.app_models.protobuf.generated import hlo_pb2 as hlo
from app.app_models.api import aeros_continuum as aeros_c
from app.utils import errors

logger = get_app_logger()

###################################################
############ aerOS Pydantic to Protobuf  ##########


def get_proto_envvars(
    payload: List[aeros_c.ServiceComponentKeyValue],
    container_type: str = "Docker",
) -> List[hlo.ServiceComponentKeyValue]:
    """
        get protobuf envVars
    """
    env_vars: List[hlo.ServiceComponentKeyValue] = []
    for item in payload:
        if container_type == "HelmChart" and item.key == "VALUES" and isinstance(item.value, dict):
            # Protobuf/REST values remain strings; avoid encoding existing JSON twice.
            arg = hlo.ServiceComponentKeyValue(key=item.key, value=json.dumps(item.value))
        elif item.value:
            arg = hlo.ServiceComponentKeyValue(key=item.key, value=item.value)
        else:
            arg = hlo.ServiceComponentKeyValue(key=item.key, value="")
        env_vars.append(arg)
    return env_vars


def get_proto_cliargs(
    payload: List[aeros_c.ServiceComponentKeyValue]
) -> List[hlo.ServiceComponentKeyValue]:
    """
        get protobuf cliArgs
    """
    cli_args: List[hlo.ServiceComponentKeyValue] = []
    for item in payload:
        if item.value:
            arg = hlo.ServiceComponentKeyValue(key=item.key, value=item.value)
        else:
            arg = hlo.ServiceComponentKeyValue(key=item.key, value="")
        cli_args.append(arg)
    return cli_args


PROTO_PROTOCOL_MAP = {
    "TCP": hlo.PortProtocol.TCP,  # pylint: disable=no-member
    "UDP": hlo.PortProtocol.UDP,  # pylint: disable=no-member
    "SCTP": hlo.PortProtocol.SCTP,  # pylint: disable=no-member
}


def get_proto_ports(payload: list[aeros_c.NetworkPort]) -> list[hlo.Port]:
    '''
    get a list of aeros_c.NetworkPort and return a list of protobuf Port messages
    
    :param payload: list of NetworkPort which at this time should be of type continuum NetworkPort
    :type payload: list[aeros_c.NetworkPort]
    :return: list of protobuf Port messages
    :rtype: list
    '''
    ports: list[hlo.Port] = []

    for p in payload:
        if not isinstance(p, aeros_c.NetworkPort):
            # at that time it should be NetworkPort, but if not, we can skip it or log a warning
            logger.warning("Expected NetworkPort, got %s. Skipping.", type(p))
            continue

        proto_protocol = PROTO_PROTOCOL_MAP.get(
            (p.portProtocol or "").upper(),
            hlo.PortProtocol.PORT_PROTOCOL_UNSPECIFIED,
        )

        ports.append(hlo.Port(
            number=p.portNumber,
            protocol=proto_protocol,
        ))

    return ports


def get_proto_service(payload: aeros_c.Service) -> hlo.Service:
    """
        get protobuf Service
    """
    service_proto = hlo.Service()
    service_proto.id = payload['id']
    return service_proto


def get_service_component_constraints(
    payload: aeros_c.InfrastructureElementRequirements
) -> hlo.ServiceComponentConstraints:
    """
        get protobuf ServiceComponentConstraints
    """
    service_component_constraints = hlo.ServiceComponentConstraints()
    # If IE is selected leave constraints out and get it from th continuum model
    if not payload.infrastructureElement:
        if payload.requiredCpuUsage:
            service_component_constraints.constraints[
                'requiredCpuUsage'] = payload.requiredCpuUsage
        if payload.requiredRam:
            service_component_constraints.constraints[
                'requiredRam'] = payload.requiredRam
        # Protobuf message ServiceComponentConstraints needs adaptation NOT map<string, float>
        # if payload.cpuArchitecture:
        #     constraints['cpuArchitecture'] = payload.cpuArchitecture
        # if payload.realTimeCapable:
        #     constraints['realTimeCapable'] = payload.realTimeCapable
        # if payload.area:
        #     constraints['area'] = payload.area
    # service_component_constraints.extend(constraints)
    # return constraints
    # print(service_component_constraints)
    return service_component_constraints


def get_proto_domain(payload: aeros_c.Domain) -> hlo.Domain:
    """
        get protobuf domain
    """
    domain_proto = hlo.Domain()
    domain_proto.id = payload.id
    return domain_proto


def get_proto_llo(
        payload: aeros_c.LowLevelOrchestrator) -> hlo.LowLevelOrchestrator:
    """
        get protobuf LLO
    """
    llo_proto = hlo.LowLevelOrchestrator()
    llo_proto.id = payload.id
    llo_proto.orchestration_type = payload.orchestrationType
    llo_proto.domain.id = get_proto_domain(payload.domain).id
    return llo_proto


def get_protobuf_ie(
        payload: aeros_c.InfrastructureElement) -> hlo.InfrastructureElement:
    """
        get protobuf ie
    """
    # print(payload)
    ie_proto = hlo.InfrastructureElement()
    ie_proto.id = payload.id
    ie_proto.total_ram = payload.ramCapacity
    ie_proto.cpu_cores = payload.cpuCores
    ie_proto.avg_power_consumption = payload.avgPowerConsumption
    ie_proto.real_time_capable = payload.realTimeCapable
    ie_proto.current_ram = payload.currentRamUsage
    ie_proto.current_cpu_usage = payload.currentCpuUsage
    ie_proto.current_power_consumption = payload.currentPowerConsumption
    ie_proto.domain.id = get_proto_domain(payload.domain).id
    ie_proto.low_level_orchestrator.MergeFrom(
        get_proto_llo(payload.lowLevelOrchestrator))
    ie_proto.hostname = payload.hostname
    ie_proto.container_technology = payload.containerTechnology
    return ie_proto


def get_service_components(
        payload: aeros_c.ServiceComponent) -> hlo.ServiceComponent:
    """
        get protobuf service components
    """
    service_components_proto = hlo.ServiceComponent()
    # Service COmponents Fields needed in all actions:
    service_components_proto.id = payload.id
    service_components_proto.image = (payload.containerResource or payload.containerImage
                                      if payload.containerType == "HelmChart" else payload.containerImage)
    service_components_proto.containerType = payload.containerType or "Docker"
    service_components_proto.namespace = payload.namespace or ""
    service_components_proto.timeout = payload.timeout or ""
    service_components_proto.service.id = payload.service  # get_proto_service(payload=payload.service)

    # Somethings are only needed when allocating
    if payload.serviceComponentStatus in [
            aeros_c.ServiceComponentStatusEnum.STARTING,
            aeros_c.ServiceComponentStatusEnum.UPDATING,
            aeros_c.ServiceComponentStatusEnum.OVERLOAD
    ]:
        service_components_proto.exposePorts = payload.exposePorts or False
        service_components_proto.isJob = payload.isJob or False
        # Set is_private flag
        service_components_proto.is_private = payload.isPrivate or False  # or default to False
        # Only set credentials if is_private is True
        if payload.isPrivate:
            service_components_proto.credentials.username = payload.repoUsername or ""
            service_components_proto.credentials.password = payload.repoPassword or ""
        service_components_proto.cliArgs.extend(
            get_proto_cliargs(payload=payload.cliArgs or []))
        service_components_proto.envVars.extend(
            get_proto_envvars(payload=payload.envVars or [], container_type=payload.containerType))
        service_components_proto.ports.extend(
            get_proto_ports(payload=payload.networkPorts or []))
        if payload.serviceComponentStatus != aeros_c.ServiceComponentStatusEnum.UPDATING:
            constraints = get_service_component_constraints(
                payload=payload.infrastructureElementRequirements)
            if constraints:
                service_components_proto.service_component_constraints.MergeFrom(
                    constraints)

    # When deleting service IE is already in service component definition
    if payload.serviceComponentStatus in [
            aeros_c.ServiceComponentStatusEnum.REMOVING,
            aeros_c.ServiceComponentStatusEnum.UPDATING,
            aeros_c.ServiceComponentStatusEnum.MIGRATING,
            aeros_c.ServiceComponentStatusEnum.OVERLOAD
    ]:
        selected_ie = get_protobuf_ie(payload=payload.infrastructureElement)
        service_components_proto.infrastructure_element.MergeFrom(selected_ie)

    return service_components_proto


def get_service_component_requirement(
    payload: aeros_c.ServiceComponentRequirement
) -> aggregator.ServiceComponentRequirement:
    """
        get protobuf Service Component Requirments
    """
    scomponent_r = aggregator.ServiceComponentRequirement()
    scomponent = get_service_components(
        payload=payload.service_component_definition)

    scomponent_r.service_component_definition.CopyFrom(scomponent)
    for item in payload.infrastructure_element_candidates:
        ie_candidate = get_protobuf_ie(item)
        scomponent_r.infrastructure_element_candidates.append(ie_candidate)
    return scomponent_r


def create_hloallocator_output(
        payload: aeros_c.HLOAllocatorOutput) -> aggregator.HLOAllocatorOutput:
    """
        Create kafka protobuf message for HLO_ALLOCATOR
    """
    message = aggregator.HLOAllocatorOutput()
    for item in payload:
        # Create a new element in the repeated field
        new_requirement = message.service_component_requirements.add()
        # Populate the new element.
        temp_requirement = get_service_component_requirement(payload=item)
        #  use MergeFrom to copy its content.
        new_requirement.MergeFrom(temp_requirement)

    # message.extend(service_component_requirements)

    return message


###################################################
############ BINARY PROTOBUF  #####################


def create_fake_feinput():
    '''
        For development, create input message
    '''
    message = aggregator.HLOFEInput()
    message.service.id = "urn:ngsi-ld:Service:05"
    return message


_producer: Optional[Producer] = None
_producer_lock = Lock()


def _get_producer() -> Producer:
    '''
    Get a singleton Kafka producer instance, initializing it if necessary.
    '''
    global _producer

    if _producer is None:
        with _producer_lock:
            if _producer is None:
                try:
                    _producer = Producer(producer_config)
                    logger.info("Kafka producer initialized")
                except Exception as ex:
                    raise errors.ServiceUnavailableError(
                        f"Failed to initialize Kafka producer: {ex}") from ex

    return _producer


def _discard_outstanding_messages(producer: Producer) -> None:
    '''
    Discard any outstanding messages in the producer's queue and in-flight messages.
    '''
    try:
        producer.purge(in_queue=True, in_flight=True, blocking=True)
    except Exception as ex:
        logger.warning("Kafka producer purge failed during reset: %s", ex)

    try:
        producer.flush(timeout=1)
    except Exception as ex:
        logger.warning("Kafka producer final flush failed during reset: %s",
                       ex)


def reset_producer() -> None:
    '''
        Reset the Kafka producer by discarding outstanding messages and reinitializing.
    '''
    global _producer

    with _producer_lock:
        if _producer is not None:
            _discard_outstanding_messages(_producer)
            _producer = None
            logger.warning("Kafka producer reset")


def produce_message(payload2allocator, timeout: float = 10.0) -> None:
    """
    Produce allocator output to Kafka and wait for delivery confirmation.
    """
    producer = _get_producer()

    delivery_error = None
    delivered = False

    def on_delivery(err, msg) -> None:
        nonlocal delivery_error, delivered

        if err is not None:
            err_str = str(err)

            if "Purged in queue" in err_str or "Purged in flight" in err_str:
                logger.warning(
                    "Kafka message purged during cleanup: %s",
                    err,
                )
                return

            delivery_error = err
            logger.error("Kafka delivery failed: %s", err)
            return

        delivered = True
        logger.info(
            "Message delivered to %s [%s] offset %s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

    message = create_hloallocator_output(payload=payload2allocator)
    logger.info("Protobuf message: %s", message)

    protobuf_msg = serialize_to_bytes(message)

    if DEV:
        logger.info("Binary message: %s", protobuf_msg)

    try:
        producer.produce(
            topic=PRODUCER_TOPIC,
            value=protobuf_msg,
            callback=on_delivery,
        )
    except BufferError as ex:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Kafka local producer queue is full: {ex}") from ex
    except KafkaException as ex:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Kafka enqueue failed: {ex}") from ex
    except Exception as ex:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Unexpected Kafka enqueue failure: {ex}") from ex

    try:
        remaining = producer.flush(timeout=timeout)
    except KafkaException as ex:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Kafka flush failed: {ex}") from ex
    except Exception as ex:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Unexpected Kafka flush failure: {ex}") from ex

    if remaining > 0:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Kafka flush timed out; {remaining} undelivered messages remain")

    if delivery_error is not None:
        reset_producer()
        raise errors.ServiceUnavailableError(
            f"Kafka delivery failed: {delivery_error}")

    if not delivered:
        reset_producer()
        raise errors.ServiceUnavailableError(
            "Kafka message was not confirmed as delivered")


def serialize_to_bytes(data_output):
    '''
        Binary Serialize string to be sent to kafka as protobuf message
    '''
    return data_output.SerializeToString()


def parse_from_bytes(data):
    '''
        Parse received kafka protobuf message from binary to string
    '''
    feinput = aggregator.HLOFEInput()
    feinput.ParseFromString(data)
    return feinput


if __name__ == "__main__":
    produce_message({'payload_key': 'payload'})

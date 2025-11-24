'''
    Protobuf and kafka related functions
'''
from typing import List
from confluent_kafka import Producer, KafkaException
from app.config import PRODUCER_TOPIC, producer_config, DEV
from app.utils.log import get_app_logger
from app.app_models.py_files import data_aggregator_pb2 as aggregator
from app.app_models.py_files import hlo_pb2 as hlo
from app.app_models import aeriOS_continuum as aeriOS_c

logger = get_app_logger()

###################################################
############ aeriOS Pydantic to Protobuf  ##########


def get_proto_envvars(
    payload: List[aeriOS_c.ServiceComponentKeyValue]
) -> List[hlo.ServiceComponentKeyValue]:
    """
        get protobuf envVars
    """
    env_vars: List[hlo.ServiceComponentKeyValue] = []
    for item in payload:
        if item.value:
            arg = hlo.ServiceComponentKeyValue(key=item.key, value=item.value)
        else:
            arg = hlo.ServiceComponentKeyValue(key=item.key, value="")
        env_vars.append(arg)
    return env_vars


def get_proto_cliargs(
    payload: List[aeriOS_c.ServiceComponentKeyValue]
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


def get_proto_ports(payload: List[aeriOS_c.NetworkPort]) -> List[hlo.Port]:
    """
        get protobuf NetworkPorts
    """
    ports: List[hlo.Port] = []
    for p in payload:
        port = hlo.Port(number=p.portNumber)
        ports.append(port)
    return ports


def get_proto_service(payload: aeriOS_c.Service) -> hlo.Service:
    """
        get protobuf Service
    """
    service_proto = hlo.Service()
    service_proto.id = payload['id']
    return service_proto


def get_service_component_constraints(
    payload: aeriOS_c.InfrastructureElementRequirements
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


def get_proto_domain(payload: aeriOS_c.Domain) -> hlo.Domain:
    """
        get protobuf domain
    """
    domain_proto = hlo.Domain()
    domain_proto.id = payload.id
    return domain_proto


def get_proto_llo(
        payload: aeriOS_c.LowLevelOrchestrator) -> hlo.LowLevelOrchestrator:
    """
        get protobuf LLO
    """
    llo_proto = hlo.LowLevelOrchestrator()
    llo_proto.id = payload.id
    llo_proto.orchestration_type = payload.orchestrationType
    llo_proto.domain.id = get_proto_domain(payload.domain).id
    return llo_proto


def get_protobuf_ie(
        payload: aeriOS_c.InfrastructureElement) -> hlo.InfrastructureElement:
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
    ie_proto.current_ram = payload.currentCpuUsage
    ie_proto.current_cpu_usage = payload.currentCpuUsage
    ie_proto.current_power_consumption = payload.currentPowerConsumption
    ie_proto.domain.id = get_proto_domain(payload.domain).id
    ie_proto.low_level_orchestrator.MergeFrom(
        get_proto_llo(payload.lowLevelOrchestrator))
    ie_proto.hostname = payload.hostname
    ie_proto.container_technology = payload.containerTechnology
    return ie_proto


def get_service_components(
        payload: aeriOS_c.ServiceComponent) -> hlo.ServiceComponent:
    """
        get protobuf service components
    """
    service_components_proto = hlo.ServiceComponent()
    # Service COmponents Fields needed in all actions:
    service_components_proto.id = payload.id
    service_components_proto.image = payload.containerImage
    service_components_proto.service.id = payload.service  # get_proto_service(payload=payload.service)

    # Somethings are only needed when allocating
    if payload.serviceComponentStatus in [
            aeriOS_c.ServiceComponentStatusEnum.STARTING,
            aeriOS_c.ServiceComponentStatusEnum.OVERLOAD
    ]:
        service_components_proto.exposePorts = payload.exposePorts
        service_components_proto.isJob = payload.isJob
        # Set is_private flag
        service_components_proto.is_private = payload.isPrivate or False  # or default to False
        # Only set credentials if is_private is True
        if payload.isPrivate:
            service_components_proto.credentials.username = payload.repoUsername or ""
            service_components_proto.credentials.password = payload.repoPassword or ""
        service_components_proto.cliArgs.extend(
            get_proto_cliargs(payload=payload.cliArgs))
        service_components_proto.envVars.extend(
            get_proto_envvars(payload=payload.envVars))
        service_components_proto.ports.extend(
            get_proto_ports(payload=payload.networkPorts))
        constraints = get_service_component_constraints(
            payload=payload.infrastructureElementRequirements)
        if constraints:
            service_components_proto.service_component_constraints.MergeFrom(
                constraints)

    # When deleting service IE is already in service component definition
    if payload.serviceComponentStatus in [
            aeriOS_c.ServiceComponentStatusEnum.REMOVING,
            aeriOS_c.ServiceComponentStatusEnum.MIGRATING,
            aeriOS_c.ServiceComponentStatusEnum.OVERLOAD
    ]:
        selected_ie = get_protobuf_ie(payload=payload.infrastructureElement)
        service_components_proto.infrastructure_element.MergeFrom(selected_ie)

    return service_components_proto


def get_service_component_requirement(
    payload: aeriOS_c.ServiceComponentRequirement
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
        payload: aeriOS_c.HLOAllocatorOutput) -> aggregator.HLOAllocatorOutput:
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


def on_delivery(err, msg):
    '''
     Callback for kafka delivering message
    '''
    if err is not None:
        logger.error('Delivery failed: %s', err)
    else:
        logger.info('Message delivered to %s [%s]', msg.topic(),
                    msg.partition())


###################################################
############ KAFKA RELATED  #######################


def produce_message(payload2allocator):
    '''
    Deliver message to redpanda
    '''
    producer = Producer(producer_config)
    # Create protobuf message for redpanda
    # According to protobuf service data model
    # And (binary) serialize it
    message = create_hloallocator_output(payload=payload2allocator)
    logger.info('Protobuf Message: %s', message)
    protobuf_msg = serialize_to_bytes(message)
    if DEV:
        logger.info('Binary message: %s', protobuf_msg)

    try:
        # Sending a message to the topic
        producer.produce(topic=PRODUCER_TOPIC,
                         value=protobuf_msg,
                         callback=on_delivery)

        # Wait for delivery confirmation
        producer.poll(5)

    except KafkaException as e:
        logger.error('Kafka Error: %s', e)

    finally:
        # Close the producer
        producer.flush()


if __name__ == "__main__":
    produce_message({'payload_key': 'payload'})

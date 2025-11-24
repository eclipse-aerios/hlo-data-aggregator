'''
    Module with all functions to create pydantic objects for aeriOS continuum entities
'''
from typing import List
from app.cb_client import CBClient
import app.app_models.aeriOS_continuum as aeriOS_C
from app.utils import continuum_utils as c_utils
from app.utils.kafka_client import produce_message
from app.utils.log import get_app_logger
from app.config import DEV

logger = get_app_logger()

cached_organizations = {}
cached_domains = {}
cached_llo = {}
cached_ie = {}


def get_aeriOS_orginization(entity_id) -> aeriOS_C.Organization:
    """Get Organization from aeriOS"""
    org_py = cached_organizations.get(entity_id, None)
    if not org_py:
        cb_client = CBClient()
        aeriOS_org_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        if aeriOS_org_json:
            org_py = aeriOS_C.Organization(**aeriOS_org_json)
        else:
            org_py = entity_id
        cached_organizations[entity_id] = org_py
    return org_py


def get_aeriOS_domain(entity_id) -> aeriOS_C.Domain:
    """Get Domain from aeriOS"""
    domain_py = cached_domains.get(entity_id)
    if not domain_py:
        cb_client = CBClient()
        aeriOS_domain_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        domain_py = aeriOS_C.Domain(**aeriOS_domain_json)
        org_py = get_aeriOS_orginization(
            domain_py.owner[0])  # FIXME, will there be many Owners ?
        domain_py.owner = org_py
        cached_domains[entity_id] = domain_py
    return domain_py


def get_aeriOS_llo(entity_id) -> aeriOS_C.LowLevelOrchestrator:
    """Get LowLevelOrchestrator from aeriOS"""
    aeriOS_llo_py = cached_llo.get(entity_id)
    if not aeriOS_llo_py:
        cb_client = CBClient()
        aeriOS_llo_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        aeriOS_llo_py = aeriOS_C.LowLevelOrchestrator(**aeriOS_llo_json)
        domain_py = get_aeriOS_domain(aeriOS_llo_py.domain)
        aeriOS_llo_py.domain = domain_py
        cached_llo[entity_id] = aeriOS_llo_py
    return aeriOS_llo_py


def get_aeriOS_ie(entity_id) -> aeriOS_C.InfrastructureElement:
    """Get infrastructure element from aeriOS"""
    if isinstance(entity_id, aeriOS_C.InfrastructureElement):
        return entity_id  # already resolved

    aeriOS_ie_py = cached_ie.get(entity_id)
    if not aeriOS_ie_py:
        cb_client = CBClient()
        aeriOS_ie_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        aeriOS_ie_py = aeriOS_C.InfrastructureElement(**aeriOS_ie_json)
        domain = get_aeriOS_domain(aeriOS_ie_py.domain)
        llo = get_aeriOS_llo(aeriOS_ie_py.lowLevelOrchestrator)
        aeriOS_ie_py.domain = domain
        aeriOS_ie_py.lowLevelOrchestrator = llo
        cached_ie[entity_id] = aeriOS_ie_py
    return aeriOS_ie_py


def get_aeriOS_scomponents_ports(ports_id_list) -> List[aeriOS_C.NetworkPort]:
    """
    Get service component ports from CB
    """
    cb_client = CBClient()
    scomponent_ports_list_py: List[aeriOS_C.NetworkPort] = []
    if ports_id_list:
        for port_id in ports_id_list:
            port_json = cb_client.query_entity(
                entity_id=port_id, ngsild_params='format=simplified')
            if port_json:
                port_py = aeriOS_C.NetworkPort(**port_json)
                scomponent_ports_list_py.append(port_py)
    return scomponent_ports_list_py


def get_service_components_requirments(
        scomponent_r_id) -> aeriOS_C.InfrastructureElementRequirements:
    """
    Get service component requirements from CB, normalize infrastructureElement field,
    and enrich with full InfrastructureElement models.
    """
    cb_client = CBClient()
    scomponent_req_json = cb_client.query_entity(
        entity_id=scomponent_r_id, ngsild_params='format=simplified')

    # Normalize infrastructureElement field (handle both old and new formats)
    if "infrastructureElement" in scomponent_req_json:
        raw_ie = scomponent_req_json["infrastructureElement"]
        if isinstance(raw_ie, dict) and "object" in raw_ie:
            scomponent_req_json["infrastructureElement"] = [raw_ie["object"]]
        elif isinstance(raw_ie, list):
            scomponent_req_json["infrastructureElement"] = [
                ie["object"] if isinstance(ie, dict) and "object" in ie else ie
                for ie in raw_ie
            ]

    # Build Pydantic model from cleaned data
    scomponent_req_py = aeriOS_C.InfrastructureElementRequirements(
        **scomponent_req_json)

    # Enrich infrastructureElement field: resolve strings to models and skip nulls
    if scomponent_req_py.infrastructureElement:
        enriched_ies = []
        ies = scomponent_req_py.infrastructureElement
        if not isinstance(ies, list):
            ies = [ies]
        for ie in ies:
            if isinstance(ie, aeriOS_C.InfrastructureElement):
                enriched_ies.append(ie)
            elif isinstance(ie, str) and ie != "urn:ngsi-ld:null":
                resolved = get_aeriOS_ie(ie)
                if resolved is not None:
                    enriched_ies.append(resolved)

        # Assign enriched list only if valid entries exist
        scomponent_req_py.infrastructureElement = enriched_ies if enriched_ies else None

    return scomponent_req_py


def get_service_components(entity_id) -> List[aeriOS_C.ServiceComponent]:
    """
    Get service components from CB
    """
    cb_client = CBClient()

    service_components_list_json = cb_client.query_entities(
        f'format=simplified&type=ServiceComponent&q=service=="{entity_id}"')
    service_components_py = [
        aeriOS_C.ServiceComponent(**item)
        for item in service_components_list_json
    ]
    for scomponent in service_components_py:
        # When removing we need to have the IE where component is allocated
        if scomponent.serviceComponentStatus in [
                aeriOS_C.ServiceComponentStatusEnum.REMOVING,
                aeriOS_C.ServiceComponentStatusEnum.OVERLOAD,
                aeriOS_C.ServiceComponentStatusEnum.MIGRATING
        ]:
            # make ie id string to IE object
            scomponent.infrastructureElement = get_aeriOS_ie(
                scomponent.infrastructureElement)

        # Somethings are only needed when allocating
        if scomponent.serviceComponentStatus in [
                aeriOS_C.ServiceComponentStatusEnum.STARTING,
                aeriOS_C.ServiceComponentStatusEnum.FAILED,
                aeriOS_C.ServiceComponentStatusEnum.OVERLOAD
        ]:
            network_ports = get_aeriOS_scomponents_ports(
                scomponent.networkPorts)
            scomponent.networkPorts = network_ports
            ie_requirements = get_service_components_requirments(
                scomponent_r_id=scomponent.infrastructureElementRequirements)
            scomponent.infrastructureElementRequirements = ie_requirements

    return service_components_py


def create_service_component_requirement(
    scomponent_py: aeriOS_C.ServiceComponent
) -> aeriOS_C.ServiceComponentRequirement:
    """
    Create pydantic modeled Service Component Requirment
    This object holds Service component object and a list of candidate IEs objects
    All objects integrated corresponf to aeriOS continuum objects.
    Objects represent ngsi-ld relationships as reltaed models inclusion.
    """
    service_component_definition = scomponent_py
    if DEV:
        logger.info("######### %s", service_component_definition)
    infrastructure_element_candidates: List[aeriOS_C.InfrastructureElement] = []
    if scomponent_py.serviceComponentStatus == aeriOS_C.ServiceComponentStatusEnum.REMOVING:
        # No candidate elements. means we are removing service component
        if DEV:
            logger.info("##### in REMOVING")
        # pass
    # Check for IE candiddates when starting or when having failed or when overload status
    elif scomponent_py.serviceComponentStatus in [
            aeriOS_C.ServiceComponentStatusEnum.STARTING,
            aeriOS_C.ServiceComponentStatusEnum.FAILED,
            aeriOS_C.ServiceComponentStatusEnum.OVERLOAD
    ]:
        if DEV:
            logger.info("##### in DEPLOYIN OR FAILED")
        # Starting allocation
        # IF Case: pre-selected IEs
        ie_list_preselected = scomponent_py.infrastructureElementRequirements.infrastructureElement
        if (scomponent_py.serviceComponentStatus
                == aeriOS_C.ServiceComponentStatusEnum.STARTING
                and isinstance(ie_list_preselected, list)
                and len(ie_list_preselected) > 0):
            # ie_list = scomponent_py.infrastructureElementRequirements.infrastructureElement
            if all(
                    isinstance(ie, str) and ie != "urn:ngsi-ld:null"
                    or isinstance(ie, aeriOS_C.InfrastructureElement)
                    for ie in ie_list_preselected):
                for ie in ie_list_preselected:
                    if isinstance(ie, aeriOS_C.InfrastructureElement):
                        infrastructure_element_candidates.append(ie)
                    elif isinstance(ie, str):
                        infrastructure_element_candidates.append(
                            get_aeriOS_ie(ie))
        # Else Case: query for matching IEs.We have requirements, either when starting or relocating or retrying failed
        else:
            q = "format=simplified&type=InfrastructureElement"
            contstraints = 'infrastructureElementStatus=="urn:ngsi-ld:InfrastructureElementStatus:Ready"'
            if scomponent_py.infrastructureElementRequirements.requiredCpuUsage:
                rcpu = scomponent_py.infrastructureElementRequirements.requiredCpuUsage
                contstraints += f"%26currentCpuUsage<{rcpu}"
            if scomponent_py.infrastructureElementRequirements.requiredRam:
                rram = scomponent_py.infrastructureElementRequirements.requiredRam
                contstraints += f"%26availableRam>{rram}"
            if scomponent_py.infrastructureElementRequirements.cpuArchitecture:
                rcpuarch = scomponent_py.infrastructureElementRequirements.cpuArchitecture  #"urn:ngsi-ld:CpuArchitecture:x64"
                contstraints += f'%26cpuArchitecture=="{rcpuarch}"'
            if scomponent_py.infrastructureElementRequirements.realTimeCapable:
                rtcap = scomponent_py.infrastructureElementRequirements.realTimeCapable
                if rtcap:
                    rcap_for_query = 'true'
                else:
                    rcap_for_query = 'false'
                contstraints += f'%26realTimeCapable=={rcap_for_query}'
            if scomponent_py.infrastructureElementRequirements.energyEfficiencyRatio:
                rnrgyratio = scomponent_py.infrastructureElementRequirements.energyEfficiencyRatio
                contstraints += f"%26energyEfficiencyRatio>={rnrgyratio}"
            # FIXME: Check continnum green ration field
            # if scomponent_py.infrastructureElementRequirements.greenEnergyRatio:
            #     greenratio = scomponent_py.infrastructureElementRequirements.greenEnergyRatio
            #     # contstraints += f"%26energyEfficiencyRatio>{greenratio}"
            if scomponent_py.infrastructureElementRequirements.domainId:
                domain_id = scomponent_py.infrastructureElementRequirements.domainId  #"urn:ngsi-ld:CpuArchitecture:x64"
                contstraints += f'%26domain=="{domain_id}"'
            if scomponent_py.infrastructureElementRequirements.area:
                rarea = scomponent_py.infrastructureElementRequirements.area
                # some mysterious query is needed here, ... leave it for later
            if contstraints:
                # add constraints query string, (but remove first %26 occurance)
                q += f'&q={contstraints}'
            logger.info("Querying for IE candidates: with query string: %s", q)
            cb_client = CBClient()
            ie_list_json: List[
                aeriOS_C.InfrastructureElement] = cb_client.query_entities(
                    ngsild_params=q)
            # We did not find IE candidates
            if not ie_list_json:
                return None
            # We found
            for ie in ie_list_json:
                infrastructure_element_candidates.append(get_aeriOS_ie(
                    ie['id']))

    r = aeriOS_C.ServiceComponentRequirement(
        service_component_definition=service_component_definition,
        infrastructure_element_candidates=infrastructure_element_candidates)
    return r


def create_hlo_allocator_py(service_id) -> aeriOS_C.HLOAllocatorOutput:
    """
    Create pydantic modeled output for Allocator
    All objects integrated correspond to aeriOS continuum objects.
    Objects represent ngsi-ld relationships as related models inclusion.
    """
    succeed = True
    try:
        scomponents_py = get_service_components(entity_id=service_id)
        service_component_requirements: List[
            aeriOS_C.ServiceComponentRequirement] = []
        for scomponent_py in scomponents_py:
            item = create_service_component_requirement(
                scomponent_py=scomponent_py)
            # Failed to create Service Component Requirments (find IE candidates)
            if not item:
                item_succeed = False
            else:
                service_component_requirements.append(item)
                item_succeed = True
            succeed &= item_succeed
    except TypeError:
        succeed = False
        logger.exception("Error buildng aeriOS continuum  pydantic entities")

    if not succeed:
        logger.info("Setting to failed state service components")
        c_utils.set_service_failed(service_id=service_id)
        return None
    return service_component_requirements


def create_scomponent_for_reorchestration(
        scomponent_id: str) -> aeriOS_C.HLOAllocatorOutput:
    '''
    Create pydantic modeled output for Allocator
    :param scomponent_id: id of the service component that needs re-orchestration
    :return: Service Component Requirments pydantic object
    '''
    # Runs as background task
    succeed = True
    cb_client = CBClient()

    service_component_requirements: List[
        aeriOS_C.ServiceComponentRequirement] = []

    try:
        service_component_json = cb_client.query_entity(
            entity_id=scomponent_id, ngsild_params='format=simplified')
        # logger.info(service_component_json)
        scomponent_py = aeriOS_C.ServiceComponent(**service_component_json)
        # print(scomponent_py)
        # We need current IE hosting the service component, to move it later to old IE
        scomponent_py.infrastructureElement = get_aeriOS_ie(
            scomponent_py.infrastructureElement)

        # Check service component is not in overload already
        if scomponent_py:
            if scomponent_py.serviceComponentStatus in [
                    aeriOS_C.ServiceComponentStatusEnum.OVERLOAD
            ]:
                logger.info(
                    "We are already reallocating this component. Stopping here!"
                )
                succeed = False
                return
            else:
                network_ports = get_aeriOS_scomponents_ports(
                    scomponent_py.networkPorts)
                scomponent_py.networkPorts = network_ports
                ie_requirements = get_service_components_requirments(
                    scomponent_r_id=scomponent_py.
                    infrastructureElementRequirements)
                scomponent_py.infrastructureElementRequirements = ie_requirements
                # Check if we found candidate IE or not.
                # FIXME: It is supposed that already hosting IE is NOT chosen in list of candidate IE as some constraint should fail
                # But for now are we sure about it? i.e. will the IE "currentCpuUsage" be updated in CB with a value of e.g. 80-90?
                # If not we must take care to manually "remove" current IE if selected ...
                scomponent_py.serviceComponentStatus = aeriOS_C.ServiceComponentStatusEnum.OVERLOAD
                item = create_service_component_requirement(
                    scomponent_py=scomponent_py)
                if not item:
                    item_succeed = False
                else:
                    service_component_requirements.append(item)
                    item_succeed = True
                succeed &= item_succeed
        else:
            logger.info("Sad but true, did not find service component: %s",
                        scomponent_id)
            succeed = False
    except TypeError:
        succeed = False
        logger.exception("Error buildng aeriOS continuum  pydantic entities")

    # FIXME: Validate with Rafa the process of not finding capable IE candidates
    # SHould we mark status as fail? and stop
    # For our demo case, i do not think we will have such a problem
    if not succeed:
        c_utils.set_service_component_status(
            scomponent_id=scomponent_py.id,
            scomponent_status=aeriOS_C.ServiceComponentStatusEnum.FAILED)
        logger.info("Setting to failed state service component: %s",
                    scomponent_py.id)
        return None

    c_utils.set_service_component_status(
        scomponent_id=scomponent_py.id,
        scomponent_status=aeriOS_C.ServiceComponentStatusEnum.OVERLOAD)
    logger.info("Setting to overload state service component: %s",
                scomponent_py.id)

    if DEV:
        logger.info(service_component_requirements)
    # return service_component_requirements

    produce_message(service_component_requirements)

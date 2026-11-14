'''
    Module with all functions to create pydantic objects for aerOS continuum entities
'''
from typing import List
from urllib.parse import urlsplit
from pydantic import ValidationError
from app.clients.cb_client import CBClient
import app.app_models.api.aeros_continuum as aeros_C
from app.services import continuum as c_utils
from app.clients.kafka_client import produce_message
from app.utils.log import get_app_logger
from app.config import DEV
from app.utils import errors
from app.services.native_patch import (validate_native_component, validate_update_identity,
                                       validate_update_domain_url, validate_native_metadata,
                                       NativeUpdateMetadataError)

logger = get_app_logger()

cached_organizations = {}
cached_domains = {}
cached_llo = {}
cached_ie = {}


def _check_update_entity(entity, entity_id, entity_type):
    """Reject redirected/missing broker results before UPDATE enrichment."""
    if (not isinstance(entity_id, str) or not entity_id or entity_id == "urn:ngsi-ld:null"
            or not isinstance(entity, dict) or entity.get("id") != entity_id
            or entity.get("type") != entity_type):
        raise errors.InvalidArgumentError(f"Invalid assigned UPDATE {entity_type} identity")


def get_aeros_orginization(entity_id, for_update=False) -> aeros_C.Organization:
    """Get Organization from AEROS"""
    org_py = None if for_update else cached_organizations.get(entity_id, None)
    if not org_py:
        cb_client = CBClient()
        aeros_org_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        if for_update:
            _check_update_entity(aeros_org_json, entity_id, "Organization")
        if aeros_org_json:
            org_py = aeros_C.Organization(**aeros_org_json)
        else:
            org_py = entity_id
        if not for_update:
            cached_organizations[entity_id] = org_py
    return org_py


def get_aeros_domain(entity_id, for_update=False) -> aeros_C.Domain:
    """Get Domain from AEROS"""
    if for_update and isinstance(entity_id, aeros_C.Domain):
        entity_id = entity_id.id
    domain_py = None if for_update else cached_domains.get(entity_id)
    if not domain_py:
        cb_client = CBClient()
        aeros_domain_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        if for_update:
            _check_update_entity(aeros_domain_json, entity_id, "Domain")
        domain_py = aeros_C.Domain(**aeros_domain_json)
        if for_update:
            if isinstance(domain_py.owner, list) and domain_py.owner:
                domain_py.owner = get_aeros_orginization(domain_py.owner[0], for_update=True)
        else:
            org_py = get_aeros_orginization(
                domain_py.owner[0])  # FIXME, will there be many Owners ?
            domain_py.owner = org_py
            cached_domains[entity_id] = domain_py
    return domain_py


def get_aeros_llo(entity_id, for_update=False) -> aeros_C.LowLevelOrchestrator:
    """Get LowLevelOrchestrator from AEROS"""
    if for_update and isinstance(entity_id, aeros_C.LowLevelOrchestrator):
        entity_id = entity_id.id
    aeros_llo_py = None if for_update else cached_llo.get(entity_id)
    if not aeros_llo_py:
        cb_client = CBClient()
        aeros_llo_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        if for_update:
            _check_update_entity(aeros_llo_json, entity_id, "LowLevelOrchestrator")
        aeros_llo_py = aeros_C.LowLevelOrchestrator(**aeros_llo_json)
        domain_py = (get_aeros_domain(aeros_llo_py.domain, for_update=True) if for_update
                     else get_aeros_domain(aeros_llo_py.domain))
        aeros_llo_py.domain = domain_py
        if not for_update:
            cached_llo[entity_id] = aeros_llo_py
    return aeros_llo_py


def get_aeros_ie(entity_id, for_update=False) -> aeros_C.InfrastructureElement:
    """Get infrastructure element from AEROS"""
    if isinstance(entity_id, aeros_C.InfrastructureElement):
        return entity_id  # already resolved

    aeros_ie_py = None if for_update else cached_ie.get(entity_id)
    if not aeros_ie_py:
        cb_client = CBClient()
        aeros_ie_json = cb_client.query_entity(
            entity_id=entity_id, ngsild_params='format=simplified')
        if for_update:
            _check_update_entity(aeros_ie_json, entity_id, "InfrastructureElement")
        aeros_ie_py = aeros_C.InfrastructureElement(**aeros_ie_json)
        domain = (get_aeros_domain(aeros_ie_py.domain, for_update=True) if for_update
                  else get_aeros_domain(aeros_ie_py.domain))
        llo = (get_aeros_llo(aeros_ie_py.lowLevelOrchestrator, for_update=True) if for_update
               else get_aeros_llo(aeros_ie_py.lowLevelOrchestrator))
        aeros_ie_py.domain = domain
        aeros_ie_py.lowLevelOrchestrator = llo
        if not for_update:
            cached_ie[entity_id] = aeros_ie_py
    return aeros_ie_py


def get_aeros_scomponents_ports(ports_id_list) -> List[aeros_C.NetworkPort]:
    """
    Get service component ports from CB
    """
    cb_client = CBClient()
    scomponent_ports_list_py: List[aeros_C.NetworkPort] = []
    if ports_id_list:
        for port_id in ports_id_list:
            port_json = cb_client.query_entity(
                entity_id=port_id, ngsild_params='format=simplified')
            if port_json:
                port_py = aeros_C.NetworkPort(**port_json)
                scomponent_ports_list_py.append(port_py)
    return scomponent_ports_list_py


def get_service_components_requirments(
        scomponent_r_id, resolve_ies=True) -> aeros_C.InfrastructureElementRequirements:
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
        elif not resolve_ies and isinstance(raw_ie, str):
            # Simplified NGSI-LD may return a single relationship as a scalar.
            # Keep it explicit even when the target later fails Helm eligibility.
            scomponent_req_json["infrastructureElement"] = [raw_ie]

    # Build Pydantic model from cleaned data
    scomponent_req_py = aeros_C.InfrastructureElementRequirements(
        **scomponent_req_json)

    # The frontend persists automatic placement as a single null relationship.
    # Normalize it even when Helm defers IE resolution for eligibility checks.
    # Other explicit lists must remain explicit, including invalid targets.
    if scomponent_req_py.infrastructureElement == ["urn:ngsi-ld:null"]:
        scomponent_req_py.infrastructureElement = None

    # Enrich infrastructureElement field: resolve strings to models and skip nulls
    if resolve_ies and scomponent_req_py.infrastructureElement:
        enriched_ies = []
        ies = scomponent_req_py.infrastructureElement
        if not isinstance(ies, list):
            ies = [ies]
        for ie in ies:
            if isinstance(ie, aeros_C.InfrastructureElement):
                enriched_ies.append(ie)
            elif isinstance(ie, str) and ie != "urn:ngsi-ld:null":
                resolved = get_aeros_ie(ie)
                if resolved is not None:
                    enriched_ies.append(resolved)

        # Assign enriched list only if valid entries exist
        scomponent_req_py.infrastructureElement = enriched_ies if enriched_ies else None

    return scomponent_req_py


def get_service_components(entity_id) -> List[aeros_C.ServiceComponent]:
    """
    Get service components from CB
    """
    cb_client = CBClient()

    service_components_list_json = cb_client.query_entities(
        f'format=simplified&type=ServiceComponent&q=service=="{entity_id}"')
    for item in service_components_list_json:
        if (isinstance(item, dict)
                and item.get("serviceComponentStatus") == aeros_C.ServiceComponentStatusEnum.UPDATING
                and item.get("containerType") in (None, "", "Docker")):
            try:
                validate_native_metadata(item)
            except ValueError as exc:
                update_ids = [entry["id"] for entry in service_components_list_json
                              if isinstance(entry, dict)
                              and entry.get("serviceComponentStatus") == aeros_C.ServiceComponentStatusEnum.UPDATING
                              and isinstance(entry.get("id"), str) and entry["id"]]
                raise NativeUpdateMetadataError(str(exc), update_ids) from exc
    service_components_py = [
        aeros_C.ServiceComponent(**item)
        for item in service_components_list_json
    ]
    updating = any(component.serviceComponentStatus == aeros_C.ServiceComponentStatusEnum.UPDATING
                   for component in service_components_py)
    for scomponent in service_components_py:
        if updating:
            # Updates use the current component assignment, never the original
            # placement requirements. Enrich in the update branch below so an
            # error can be handled without failing unrelated service components.
            # Untouched components must not perform retry/placement work either.
            continue
        # When removing we need to have the IE where component is allocated
        if scomponent.serviceComponentStatus in [
                aeros_C.ServiceComponentStatusEnum.REMOVING,
                aeros_C.ServiceComponentStatusEnum.OVERLOAD,
                aeros_C.ServiceComponentStatusEnum.MIGRATING
        ]:
            # make ie id string to IE object
            scomponent.infrastructureElement = get_aeros_ie(
                scomponent.infrastructureElement)

        # Somethings are only needed when allocating
        if scomponent.serviceComponentStatus in [
                aeros_C.ServiceComponentStatusEnum.STARTING,
                aeros_C.ServiceComponentStatusEnum.FAILED,
                aeros_C.ServiceComponentStatusEnum.OVERLOAD
        ]:
            network_ports = get_aeros_scomponents_ports(
                scomponent.networkPorts)
            scomponent.networkPorts = network_ports
            ie_requirements = get_service_components_requirments(
                scomponent_r_id=scomponent.infrastructureElementRequirements,
                resolve_ies=not (scomponent.containerType == "HelmChart" and
                                scomponent.serviceComponentStatus == aeros_C.ServiceComponentStatusEnum.STARTING))
            scomponent.infrastructureElementRequirements = ie_requirements

    return service_components_py


def is_helm_candidate(ie: aeros_C.InfrastructureElement) -> bool:
    """Helm uses a Kubernetes IE to identify a routable destination domain."""
    if not isinstance(ie, aeros_C.InfrastructureElement) or ie.containerTechnology != "Kubernetes":
        return False
    domain = ie.domain
    llo = ie.lowLevelOrchestrator
    if not isinstance(domain, aeros_C.Domain) or not isinstance(llo, aeros_C.LowLevelOrchestrator):
        return False
    llo_domain_id = llo.domain.id if isinstance(llo.domain, aeros_C.Domain) else llo.domain
    try:
        url = urlsplit(domain.publicUrl)
    except ValueError:
        return False
    return bool(ie.id and ie.hostname and domain.id and llo.id and
                llo_domain_id == domain.id and url.scheme in ("http", "https") and url.netloc)


def get_helm_candidate(ie):
    """Resolve candidate metadata, treating missing routing data as ineligible."""
    try:
        resolved = get_aeros_ie(ie)
    except (ValidationError, TypeError, KeyError, IndexError, errors.ResourceNotFoundError):
        logger.warning("Helm candidate %s has incomplete IE/domain metadata", ie)
        return None
    return resolved if is_helm_candidate(resolved) else None


def create_service_component_requirement(
    scomponent_py: aeros_C.ServiceComponent
) -> aeros_C.ServiceComponentRequirement:
    """
    Create pydantic modeled Service Component Requirment
    This object holds Service component object and a list of candidate IEs objects
    All objects integrated corresponf to aerOS continuum objects.
    Objects represent ngsi-ld relationships as reltaed models inclusion.
    """
    service_component_definition = scomponent_py
    if DEV:
        logger.info("######### %s", service_component_definition)
    infrastructure_element_candidates: List[aeros_C.InfrastructureElement] = []
    helm_placement = (scomponent_py.containerType == "HelmChart" and
                      scomponent_py.serviceComponentStatus == aeros_C.ServiceComponentStatusEnum.STARTING)
    if scomponent_py.serviceComponentStatus == aeros_C.ServiceComponentStatusEnum.UPDATING:
        try:
            validate_update_identity(scomponent_py)
        except ValueError:
            logger.exception("Invalid persisted identity for update %s", scomponent_py.id)
            return None
        if scomponent_py.containerType not in (None, "", "Docker", "HelmChart"):
            logger.warning("Unsupported stored workload type for update %s", scomponent_py.id)
            return None
        if scomponent_py.containerType != "HelmChart":
            try:
                validate_native_component(scomponent_py)
            except ValueError:
                logger.exception("Invalid native update metadata for %s", scomponent_py.id)
                return None
        assignment = scomponent_py.infrastructureElement
        assigned_id = assignment.id if isinstance(assignment, aeros_C.InfrastructureElement) else assignment
        if not assigned_id or assigned_id == "urn:ngsi-ld:null":
            logger.warning("Updating component %s has no current assignment", scomponent_py.id)
            return None
        try:
            assigned_ie = get_aeros_ie(assignment, for_update=True)
        except (ValidationError, TypeError, KeyError, IndexError, errors.aeriosError):
            logger.exception("Unable to resolve current assignment for update %s", scomponent_py.id)
            return None
        if (not isinstance(assigned_ie, aeros_C.InfrastructureElement)
                or assigned_ie.type != "InfrastructureElement"
                or assigned_ie.id != assigned_id or not assigned_ie.hostname
                or not isinstance(assigned_ie.domain, aeros_C.Domain)
                or not isinstance(assigned_ie.lowLevelOrchestrator, aeros_C.LowLevelOrchestrator)):
            logger.warning("Updating component %s has incomplete assignment metadata", scomponent_py.id)
            return None
        if (assigned_ie.containerTechnology not in ("Docker", "Kubernetes")
                or (scomponent_py.containerType == "HelmChart"
                    and assigned_ie.containerTechnology != "Kubernetes")):
            logger.warning("Workload and assigned backend do not support update %s", scomponent_py.id)
            return None
        domain = assigned_ie.domain
        llo = assigned_ie.lowLevelOrchestrator
        llo_domain_id = llo.domain.id if isinstance(llo.domain, aeros_C.Domain) else llo.domain
        try:
            validate_update_domain_url(domain.publicUrl)
        except ValueError:
            return None
        if (not domain.id or domain.id == "urn:ngsi-ld:null"
                or not llo.id or llo.id == "urn:ngsi-ld:null"
                or llo_domain_id != domain.id):
            logger.warning("Updating component %s has invalid domain/LLO routing", scomponent_py.id)
            return None
        try:
            if scomponent_py.networkPorts and all(
                    isinstance(port, aeros_C.NetworkPort) for port in scomponent_py.networkPorts):
                ports = scomponent_py.networkPorts
            else:
                ports = (get_aeros_scomponents_ports(scomponent_py.networkPorts)
                         if scomponent_py.networkPorts else [])
            if scomponent_py.networkPorts and len(ports) != len(scomponent_py.networkPorts):
                logger.warning("Updating component %s has unresolved port metadata", scomponent_py.id)
                return None
        except (ValidationError, TypeError, KeyError, IndexError, errors.aeriosError):
            logger.exception("Unable to resolve update ports for %s", scomponent_py.id)
            return None
        service_component_definition.infrastructureElement = assigned_ie
        service_component_definition.networkPorts = ports
        # No initial-placement filtering, discovery or candidate selection on updates.
    elif scomponent_py.serviceComponentStatus == aeros_C.ServiceComponentStatusEnum.REMOVING:
        # No candidate elements. means we are removing service component
        if DEV:
            logger.info("##### in REMOVING")
        # pass
    # Check for IE candiddates when starting or when having failed or when overload status
    elif scomponent_py.serviceComponentStatus in [
            aeros_C.ServiceComponentStatusEnum.STARTING,
            aeros_C.ServiceComponentStatusEnum.FAILED,
            aeros_C.ServiceComponentStatusEnum.OVERLOAD
    ]:
        if DEV:
            logger.info("##### in DEPLOYIN OR FAILED")
        # Starting allocation
        # IF Case: pre-selected IEs
        ie_list_preselected = scomponent_py.infrastructureElementRequirements.infrastructureElement
        if (scomponent_py.serviceComponentStatus
                == aeros_C.ServiceComponentStatusEnum.STARTING
                and isinstance(ie_list_preselected, list)
                and len(ie_list_preselected) > 0):
            # ie_list = scomponent_py.infrastructureElementRequirements.infrastructureElement
            if all(
                    isinstance(ie, str) and ie != "urn:ngsi-ld:null"
                    or isinstance(ie, aeros_C.InfrastructureElement)
                    for ie in ie_list_preselected):
                for ie in ie_list_preselected:
                    if helm_placement:
                        candidate = get_helm_candidate(ie)
                        if candidate is not None:
                            infrastructure_element_candidates.append(candidate)
                    elif isinstance(ie, aeros_C.InfrastructureElement):
                        infrastructure_element_candidates.append(ie)
                    elif isinstance(ie, str):
                        infrastructure_element_candidates.append(
                            get_aeros_ie(ie))
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
                aeros_C.InfrastructureElement] = cb_client.query_entities(
                    ngsild_params=q)
            # We did not find IE candidates
            if not ie_list_json:
                return None
            # We found
            for ie in ie_list_json:
                if helm_placement:
                    candidate = get_helm_candidate(ie['id'])
                    if candidate is not None:
                        infrastructure_element_candidates.append(candidate)
                else:
                    infrastructure_element_candidates.append(get_aeros_ie(ie['id']))

    if helm_placement and not infrastructure_element_candidates:
        logger.warning("No Kubernetes IE with domain routing is available for Helm component %s", scomponent_py.id)
        return None

    r = aeros_C.ServiceComponentRequirement(
        service_component_definition=service_component_definition,
        infrastructure_element_candidates=infrastructure_element_candidates)
    return r


def create_hlo_allocator_py(service_id) -> aeros_C.HLOAllocatorOutput:
    """
    Create pydantic modeled output for Allocator
    All objects integrated correspond to aerOS continuum objects.
    Objects represent ngsi-ld relationships as related models inclusion.
    """
    succeed = True
    updating_components = []
    invalid_update_ids = None
    try:
        scomponents_py = get_service_components(entity_id=service_id)
        updating_components = [component for component in scomponents_py
                               if component.serviceComponentStatus == aeros_C.ServiceComponentStatusEnum.UPDATING]
        service_component_requirements: List[
            aeros_C.ServiceComponentRequirement] = []
        for scomponent_py in scomponents_py:
            if updating_components and scomponent_py.serviceComponentStatus != aeros_C.ServiceComponentStatusEnum.UPDATING:
                # A service-ID update event applies only to its UPDATING targets.
                # Omitted components retain their statuses and existing allocation.
                continue
            if updating_components:
                try:
                    validate_update_identity(scomponent_py, service_id)
                except ValueError:
                    logger.exception("Update component does not belong to requested Service %s", service_id)
                    succeed = False
                    continue
            item = create_service_component_requirement(scomponent_py=scomponent_py)
            # Failed to create Service Component Requirments (find IE candidates)
            if not item:
                item_succeed = False
            else:
                service_component_requirements.append(item)
                item_succeed = True
            succeed &= item_succeed
    except NativeUpdateMetadataError as exc:
        succeed = False
        invalid_update_ids = exc.component_ids
        logger.exception("Invalid retained native metadata for requested update")
    except TypeError:
        succeed = False
        logger.exception("Error buildng aerOS continuum  pydantic entities")

    if not succeed:
        if updating_components or invalid_update_ids is not None:
            logger.info("Setting only requested update components to failed state")
            update_ids = (invalid_update_ids if invalid_update_ids is not None
                          else [component.id for component in updating_components])
            for component_id in update_ids:
                try:
                    c_utils.set_service_component_status(
                        scomponent_id=component_id,
                        scomponent_status=aeros_C.ServiceComponentStatusEnum.FAILED)
                except errors.aeriosError:
                    # Do not let the consumer's all-service rollback affect
                    # unrelated components if this targeted rollback also fails.
                    logger.exception("Unable to mark failed update component %s", component_id)
        else:
            logger.info("Setting to failed state service components")
            c_utils.set_service_failed(service_id=service_id)
        return None
    return service_component_requirements


def create_scomponent_for_reorchestration(
        scomponent_id: str) -> aeros_C.HLOAllocatorOutput:
    '''
    Create pydantic modeled output for Allocator
    :param scomponent_id: id of the service component that needs re-orchestration
    :return: Service Component Requirments pydantic object
    '''
    # Runs as background task
    succeed = True
    cb_client = CBClient()

    service_component_requirements: List[
        aeros_C.ServiceComponentRequirement] = []

    try:
        service_component_json = cb_client.query_entity(
            entity_id=scomponent_id, ngsild_params='format=simplified')
        # logger.info(service_component_json)
        scomponent_py = aeros_C.ServiceComponent(**service_component_json)
        # print(scomponent_py)
        # We need current IE hosting the service component, to move it later to old IE
        scomponent_py.infrastructureElement = get_aeros_ie(
            scomponent_py.infrastructureElement)

        # Check service component is not in overload already
        if scomponent_py:
            if scomponent_py.serviceComponentStatus in [
                    aeros_C.ServiceComponentStatusEnum.OVERLOAD
            ]:
                logger.info(
                    "We are already reallocating this component. Stopping here!"
                )
                succeed = False
                return
            else:
                network_ports = get_aeros_scomponents_ports(
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
                scomponent_py.serviceComponentStatus = aeros_C.ServiceComponentStatusEnum.OVERLOAD
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
        logger.exception("Error buildng aerOS continuum  pydantic entities")

    # FIXME: Validate with Rafa the process of not finding capable IE candidates
    # SHould we mark status as fail? and stop
    # For our demo case, i do not think we will have such a problem
    if not succeed:
        c_utils.set_service_component_status(
            scomponent_id=scomponent_py.id,
            scomponent_status=aeros_C.ServiceComponentStatusEnum.FAILED)
        logger.info("Setting to failed state service component: %s",
                    scomponent_py.id)
        return None

    c_utils.set_service_component_status(
        scomponent_id=scomponent_py.id,
        scomponent_status=aeros_C.ServiceComponentStatusEnum.OVERLOAD)
    logger.info("Setting to overload state service component: %s",
                scomponent_py.id)

    if DEV:
        logger.info(service_component_requirements)
    # return service_component_requirements

    produce_message(service_component_requirements)

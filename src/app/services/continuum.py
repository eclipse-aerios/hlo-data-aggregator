'''
  Module with funcions to check or update continuum state represantations
'''
from typing import List
from app.clients.cb_client import CBClient
from app.app_models.api.aeros_continuum import ServiceComponentStatusEnum as status
from app.app_models.api.aeros_continuum import ServiceActionTypeEnum
# from app.utils.log import get_app_logger


def check_service_exists(service_id: str) -> bool:
    '''
    Check if service  exists
    :param  service_id: id of the service of which part is service component
    :return True or False
    '''
    cb_client = CBClient()
    jsonld_params = f'format=simplified&q=service=="{service_id}"'
    service_json = cb_client.query_entity(entity_id=service_id,
                                          ngsild_params=jsonld_params)
    if service_json is not None and service_json.get('type') is not None:
        return True
    return False


def check_service_component_exists(service_id: str,
                                   service_component_id: str) -> bool:
    '''
    Check if service component exists
    :param  service_id: id of the service of which part is service component
    :param  service_component_id: id of the service component id
    :return True or False
    '''
    cb_client = CBClient()
    jsonld_params = f'format=simplified&q=service=="{service_id}"'
    scomponent_json = cb_client.query_entity(entity_id=service_component_id,
                                             ngsild_params=jsonld_params)
    if scomponent_json is not None and scomponent_json.get(
            'serviceComponentStatus') is not None:
        if scomponent_json.get('serviceComponentStatus') == status.RUNNING:
            return True
    return False


def get_service_components_list(entity_id: str) -> List[str]:
    """
        Get a list with all ids of service components of a service
    """
    cb_client = CBClient()
    scomponents_id_list = []
    service_components_list_json = cb_client.query_entities(
        f'format=simplified&type=ServiceComponent&q=service=="{entity_id}"')
    for scomponent in service_components_list_json:
        scomponents_id_list.append(scomponent.get('id'))
    return scomponents_id_list


def set_all_service_components_failed(service_id: str) -> None:
    '''
    Set all service components of a service to failed status in CB
    
    :param service_id: The service ID for which all components should be set to failed
    :type service_id: str
    '''
    for scomponent_id in get_service_components_list(service_id):
        set_service_component_status(scomponent_id, status.FAILED)


def set_service_action_type(service_id: str, action_type: str) -> None:
    '''
    Set the action type of a service in CB
    
    :param service_id: The service ID for which the action type should be set
    :type service_id: str
    :param action_type: The action type to set (e.g., 'DEPLOYING', 'DESTROYING', 'FINISHED')
    :type action_type: str
    '''
    cb_client = CBClient()
    data = {
        "actionType": {
            "type": "Property",
            "value": action_type,
        }
    }
    cb_client.patch_entity(entity_id=service_id, upd_object=data)


def set_service_failed(service_id: str) -> None:
    """
    Mark a service as failed:
    - set all its service components to FAILED
    - set the service actionType to FINISHED

    This is used as a rollback when service allocation fails.
    """
    set_all_service_components_failed(service_id)
    set_service_action_type(service_id, ServiceActionTypeEnum.FINISHED)


def set_service_component_status(scomponent_id, scomponent_status: str):
    """
        Set the status for a service component in CB
    """
    cb_client = CBClient()
    data = {
        "serviceComponentStatus": {
            "type": "Relationship",
            "object": scomponent_status
        }
    }
    cb_client.patch_entity(entity_id=scomponent_id, upd_object=data)


def set_service_component_status_attr(scomponent_id, scomponent_status: str):
    """
        Set the status for a service component in CB
    """
    cb_client = CBClient()

    data = {'type': 'Relationship', 'object': scomponent_status}
    cb_client.patch_entity_attr(entity_id=scomponent_id,
                                attr='serviceComponentStatus',
                                upd_object=data)


def set_service_component_ie(scomponent_id, allocated_ie_id: str):
    """
        Update IE for Service Component
        Create relationship upon allocation
        Delete relationship upon deallocation
    """
    cb_client = CBClient()
    data = {
        "infrastructureElement": {
            "type": "Relationship",
            "object": allocated_ie_id
        }
    }
    cb_client.patch_entity(entity_id=scomponent_id, upd_object=data)


def set_service_component_ie_attr(scomponent_id, allocated_ie_id: str):
    """
        Update IE for Service Component
        Create relationship upon allocation
        Delete relationship upon deallocation
    """
    cb_client = CBClient()
    data = {
        "infrastructureElement": {
            "type": "Relationship",
            "object": allocated_ie_id
        }
    }
    data = {'type': 'Relationship', 'value': allocated_ie_id}
    cb_client.patch_entity_attr(entity_id=scomponent_id,
                                attr='infrastructureElement',
                                upd_object=data)

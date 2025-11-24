'''
  Module with funcions to check or update continuum state represantations
'''
from app.cb_client import CBClient
from app.app_models.aeriOS_continuum import ServiceComponentStatusEnum as status
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


def set_service_failed(service_id):
    """
    Set all service components of a service to failed state.
    """
    cb_client = CBClient()
    service_components_list_json = cb_client.query_entities(
        f'format=simplified&type=ServiceComponent&q=service=="{service_id}"&attrs=serviceComponentStatus'
    )
    for scomponent in service_components_list_json:
        set_service_component_status(scomponent_id=scomponent["id"],
                                     scomponent_status=status.FAILED)


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


def set_service_component_status_attr(scomponent_id,
                                      scomponent_status: str):
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


def set_service_component_ie_attr(scomponent_id,
                                  allocated_ie_id: str):
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

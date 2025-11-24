'''
 Docstring
'''
import json
import requests
import app.config as config
from app.utils.decorators import catch_requests_exceptions
from app import k8s_shim_client


class CBClient:
    '''
        Client to query CB
          query entities/{entity_id}
             or
          query entities/
        ... ngsi-ld url params welcome
          patch entity 
    '''

    def __init__(self):
        self.api_url = config.API_URL
        self.api_port = config.API_PORT
        self.url_version = config.URL_VERSION
        self.m2m_cb_token = k8s_shim_client.get_m2m_cb_token()
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'aeriOS': 'true',
            'Authorization': f'Bearer {self.m2m_cb_token}'
        }

    @catch_requests_exceptions
    def query_entity(self, entity_id, ngsild_params) -> dict:
        '''
            Query entity with ngsi-ld params
            :input
            @param entity_id: the id of the queried entity
            @param ngsi-ld: the query params
            :output
            ngsi-ld object
        '''
        entity_url = f'{self.api_url}:{self.api_port}/{self.url_version}entities/{entity_id}?{ngsild_params}'
        response = requests.get(entity_url, headers=self.headers, timeout=15)
        response.raise_for_status()
        return response.json()

    @catch_requests_exceptions
    def query_entities(self, ngsild_params):
        '''
            Query entities with ngsi-ld params
            :input
            @param ngsi-ld: the query params
            :output
            ngsi-ld object
        '''
        entity_url = f"{self.api_url}:{self.api_port}/{self.url_version}entities?{ngsild_params}"
        response = requests.get(entity_url, headers=self.headers, timeout=15)
        response.raise_for_status()
        return response.json()

    @catch_requests_exceptions
    def patch_entity(self, entity_id, upd_object: dict) -> dict:
        '''
            Upadte entity in aeriOS contiunuum
            :input
            @param entity_id: the id of the queried entity
            @param upd_object: the  json object to update the entity with
            :output
            
        '''
        entity_url = f'{self.api_url}:{self.api_port}/{self.url_version}entities/{entity_id}'
        response = requests.patch(entity_url,
                                  headers=self.headers,
                                  data=json.dumps(upd_object),
                                  timeout=15)
        response.raise_for_status()
        return response.status_code

    @catch_requests_exceptions
    def patch_entity_attr(self, entity_id, attr, upd_object: dict) -> dict:
        '''
            Do NOT use this one, prefer patch above
            Upadte entity in aeriOS contiunuum
            :input
            @param entity_id: the id of the queried entity
            @attr: the attribute to be updated
            @param upd_object: the  json object to update the entity with
            :output
            
        '''
        entity_url = f'{self.api_url}:{self.api_port}/{self.url_version}entities/{entity_id}/attrs/{attr}'
        response = requests.patch(entity_url,
                                  headers=self.headers,
                                  data=json.dumps(upd_object),
                                  timeout=15)
        response.raise_for_status()
        return response.status_code

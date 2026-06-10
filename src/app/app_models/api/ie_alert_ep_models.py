'''
Pydantic models for REST endpoints for IE resource alerting
'''
from pydantic import BaseModel

class IEAlert(BaseModel):
    '''
    Alert input
    '''
    serviceComponentId: str
    event: str
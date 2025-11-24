'''
Module to query aeriOS service for retrieving token for m2m communication.
Used when accessing CB, even when accessing it internally 
   as this is used to propagate federation requests to other Orion-LD brokers
Used also for accessing Deployment engine local allocation manager 
   for submitting final pod placements
'''
import requests
from app.utils.decorators import catch_requests_exceptions
from app.utils.log import get_app_logger
from app.config import TOKEN_URL

logger = get_app_logger()


@catch_requests_exceptions
def get_m2m_cb_token():
    '''
    Get m2m token for Orion-LD queries
    '''
    url = f"{TOKEN_URL}/cb"

    # Make a GET request to the endpoint
    response = requests.get(url=url, timeout=2)

    # Raise an exception for HTTP errors
    response.raise_for_status()

    # Parse the JSON response from the server
    token_data = response.json()
    token_value = token_data.get("token")

    # Return the token value if it exists, else return None
    if token_value:
        return token_value
    else:
        logger.info("Token value not found in response.")
        return None


def get_m2m_hlo_token():
    '''
    Get m2m token for HLO Local Allocation Engine queries
    '''
    url = f"{TOKEN_URL}/hlo"

    # Make a GET request to the endpoint
    response = requests.get(url=url, timeout=2)

    # Raise an exception for HTTP errors
    response.raise_for_status()

    # Raise an exception for HTTP errors
    response.raise_for_status()

    # Parse the JSON response from the server
    token_data = response.json()
    token_value = token_data.get("token")

    # Return the token value if it exists, else return None
    if token_value:
        return token_value
    else:
        logger.info("Token value not found in response.")
        return None

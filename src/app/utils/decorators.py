'''
Docstring
'''
from requests.exceptions import RequestException, HTTPError, Timeout
from app.utils.log import get_app_logger


def catch_requests_exceptions(func):
    '''
        Docstring
    '''
    logger = get_app_logger()

    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except HTTPError as e:
            logger.info("4xx or 5xx: %s \n", {e})
            return None  # raise our custom exception or log, etc.
        except ConnectionError as e:
            logger.info(
                "Raised for connection-related issues (e.g., DNS resolution failure, network issues): %s \n",
                {e})
            return None  # raise our custom exception or log, etc.
        except Timeout as e:
            logger.info("Timeout occured: %s \n", {e})
            return None  # raise our custom exception or log, etc.
        except RequestException as e:
            logger.info("Request failed: %s \n", {e})
            return None  # raise our custom exception or log, etc.

    return wrapper


# import json
# from requests.models import Response
# from requests.exceptions import RequestException, HTTPError, Timeout, SSLError
# from app.utils.log import get_app_logger

# def catch_requests_exceptions(func):
#     """
#     Decorator to handle requests exceptions gracefully.
#     Returns a consistent `Response` object for both success and error cases.
#     """
#     logger = get_app_logger()

#     def wrapper(*args, **kwargs) -> Response:
#         try:
#             # Call the decorated function
#             result = func(*args, **kwargs)
#             return result  # Success: Return the actual response object
#         except SSLError as e:
#             logger.error(
#                 "SSL verification failed. Retrying with verify=False... %s", e)
#             if 'verify' not in kwargs or kwargs['verify'] is not False:
#                 kwargs['verify'] = False
#                 try:
#                     return func(*args, **kwargs)
#                 except Exception as retry_error:
#                     logger.error("Retry failed after SSL exception: %s",
#                                  retry_error)
#             return create_fake_response(
#                 500, {"error": "SSL verification failed, retry unsuccessful."})
#         except HTTPError as e:
#             logger.error("HTTP error (4xx/5xx) occurred: %s", e)
#             status_code = e.response.status_code if e.response else 500
#             error_response = extract_json_safe(e.response)
#             return create_fake_response(status_code, error_response)
#         except ConnectionError as e:
#             logger.error("Connection error: %s", e)
#             return create_fake_response(
#                 503, {"error": "Connection error occurred."})
#         except Timeout as e:
#             logger.error("Request timed out: %s", e)
#             return create_fake_response(504,
#                                         {"error": "Request timeout occurred."})
#         except RequestException as e:
#             logger.error("General RequestException: %s", e)
#             return create_fake_response(
#                 500, {"error": "A request exception occurred."})
#         except Exception as e:
#             logger.error("Unexpected exception: %s", e)
#             return create_fake_response(
#                 500, {"error": "An unexpected error occurred."})

#     return wrapper

# def create_fake_response(status_code: int, json_body: dict) -> Response:
#     """
#     Creates a fake `requests.Response` object with the specified status code and JSON body.
#     """
#     fake_response = Response()
#     fake_response.status_code = status_code
#     fake_response._content = str.encode(
#         json.dumps(json_body))  # Store JSON body as bytes
#     fake_response.headers[
#         "Content-Type"] = "application/json"  # Set content type
#     return fake_response

# def extract_json_safe(response: Response) -> dict:
#     """
#     Extracts JSON from a Response object safely. Falls back to text if JSON is not valid.
#     """
#     try:
#         return response.json() if response else {
#             "error": "HTTP error occurred."
#         }
#     except ValueError:
#         return {"error": response.text if response else "HTTP error occurred."}

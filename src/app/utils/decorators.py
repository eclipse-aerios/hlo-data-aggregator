"""
Decorators for handling request exceptions in app functions.
"""

from functools import wraps
from requests.exceptions import (
    HTTPError,
    RequestException,
    Timeout,
    SSLError,
    ConnectionError as RequestsConnectionError,
)

from app.utils.log import get_app_logger
from app.utils import errors


def catch_requests_exceptions(func):
    """
    Decorator to catch requests exceptions and translate them into app errors.
    """
    logger = get_app_logger()

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)

        except SSLError as e:
            logger.warning(
                "SSL verification failed for %s. Retrying with verify=False: %s",
                func.__name__,
                e,
            )
            retry_kwargs = dict(kwargs)
            retry_kwargs["verify"] = False

            try:
                return func(*args, **retry_kwargs)
            except HTTPError as retry_e:
                resp = getattr(retry_e, "response", None)
                status_code = getattr(resp, "status_code", None)
                payload = getattr(resp, "text", "") if resp is not None else ""
                raise _map_http_error(status_code, payload,
                                      retry_e) from retry_e
            except Timeout as retry_e:
                raise errors.ServiceUnavailableError(
                    "Request timed out") from retry_e
            except RequestsConnectionError as retry_e:
                raise errors.ServiceUnavailableError(
                    "Failed to connect to upstream service") from retry_e
            except RequestException as retry_e:
                raise errors.ServiceUnavailableError(
                    f"Upstream request failed: {retry_e}") from retry_e

        except HTTPError as e:
            resp = getattr(e, "response", None)
            status_code = getattr(resp, "status_code", None)
            payload = getattr(resp, "text", "") if resp is not None else ""
            logger.error("HTTP error in %s: %s %s", func.__name__, status_code,
                         e)
            raise _map_http_error(status_code, payload, e) from e

        except Timeout as e:
            logger.warning("Timeout in %s: %s", func.__name__, e)
            raise errors.ServiceUnavailableError("Request timed out") from e

        except RequestsConnectionError as e:
            logger.warning("Connection error in %s: %s", func.__name__, e)
            raise errors.ServiceUnavailableError(
                "Failed to connect to upstream service") from e

        except RequestException as e:
            logger.error("Request failed in %s: %s", func.__name__, e)
            raise errors.ServiceUnavailableError(
                "Upstream request failed") from e

    return wrapper


def _map_http_error(status_code, payload, original_exc):
    if status_code == 400:
        return errors.InvalidArgumentError(f"Bad request: {payload}")
    if status_code == 401:
        return errors.UnauthenticatedError(f"Unauthorized: {payload}")
    if status_code == 403:
        return errors.PermissionDeniedError(f"Forbidden: {payload}")
    if status_code == 404:
        return errors.ResourceNotFoundError(f"Not found: {payload}")
    if status_code == 409:
        return errors.ConflictError(f"Conflict: {payload}")
    if status_code == 503:
        return errors.ServiceUnavailableError(
            f"Service unavailable: {payload}")
    return errors.aeriosError(f"Unhandled HTTP error {status_code}: {payload}")

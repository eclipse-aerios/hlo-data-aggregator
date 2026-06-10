# -*- coding: utf-8 -*-
"""
Custom exception hierarchy for the Edge Cloud Platform.
Each subclass represents a specific HTTP-style error condition.
"""


class aeriosError(Exception):
    """Base exception for aerIOS errors."""


class InvalidArgumentError(aeriosError):
    """400 Bad Request"""


class UnauthenticatedError(aeriosError):
    """401 Unauthorized"""


class PermissionDeniedError(aeriosError):
    """403 Forbidden"""


class ResourceNotFoundError(aeriosError):
    """404 Not Found"""


class ConflictError(aeriosError):
    """409 Conflict"""


class ServiceUnavailableError(aeriosError):
    """503 Service Unavailable"""

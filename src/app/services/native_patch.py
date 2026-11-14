"""Pure validation of effective native UPDATE data; no CREATE or placement rules."""
import re
from urllib.parse import urlsplit


class NativeUpdateMetadataError(ValueError):
    """Retained native metadata failed before model coercion for an UPDATE."""

    def __init__(self, message, component_ids):
        super().__init__(message)
        self.component_ids = component_ids


def validate_update_domain_url(value):
    """Validate an UPDATE destination base URL before any domain dispatch."""
    error = "Assigned domain has no valid destination URL"
    if (not isinstance(value, str) or not value
            or any(character.isspace() for character in value)
            or any(character in value for character in ("\x00", "\\", "?", "#"))):
        raise ValueError(error)
    try:
        parsed = urlsplit(value)
        port = parsed.port
        if (parsed.scheme not in ("http", "https") or not parsed.netloc or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.netloc.endswith(":")
                or (port is not None and not 1 <= port <= 65535)):
            raise ValueError(error)
    except ValueError as exc:
        raise ValueError(error) from exc
    return value


def validate_update_identity(component, service_id=None):
    """Check the persisted canonical component identity without renaming it."""
    service = component.service.id if hasattr(component.service, "id") else component.service
    prefix = f"{service}:Component:"
    suffix = component.id[len(prefix):] if component.id.startswith(prefix) else ""
    if (not isinstance(service, str) or not service
            or (service_id is not None and service != service_id)
            or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", suffix)):
        raise ValueError("PATCH requires the existing canonical component and Service membership")


def validate_native_image_reference(image):
    """Match frontend PATCH reference structure without registry IO or Helm parsing."""
    error = "Native image must be a complete container image reference"
    if (not isinstance(image, str) or not image
            or not re.fullmatch(r"[A-Za-z0-9._+:/@\[\]-]+", image)
            or "://" in image or image.count("@") > 1):
        raise ValueError(error)
    name_tag, digest_separator, digest = image.partition("@")
    if digest_separator:
        algorithm, separator, encoded = digest.partition(":")
        if (not separator or not re.fullmatch(r"[a-z][a-z0-9_+.-]*", algorithm)
                or not re.fullmatch(r"[a-fA-F0-9]{32,}", encoded)):
            raise ValueError(error)
        digest_lengths = {"sha256": 64, "sha384": 96, "sha512": 128}
        if algorithm in digest_lengths and len(encoded) != digest_lengths[algorithm]:
            raise ValueError(error)
    if name_tag.rfind(":") > name_tag.rfind("/"):
        name, tag = name_tag.rsplit(":", 1)
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
            raise ValueError(error)
    else:
        name = name_tag
    components = name.split("/")
    if not name or any(not component for component in components):
        raise ValueError(error)
    if (len(components) > 1 and ("." in components[0] or ":" in components[0]
                                or components[0] == "localhost" or components[0].startswith("["))):
        registry = components.pop(0)
        if registry.startswith("["):
            import ipaddress
            host, bracket, suffix = registry[1:].partition("]")
            try:
                ipaddress.IPv6Address(host)
            except ValueError as exc:
                raise ValueError(error) from exc
            if not bracket or (suffix and not suffix.startswith(":")):
                raise ValueError(error)
            port = suffix[1:] if suffix else None
        else:
            host, separator, port_value = registry.partition(":")
            if any(not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", part)
                   for part in host.split(".")):
                raise ValueError(error)
            port = port_value if separator else None
        if port is not None and (not port.isascii() or not port.isdigit()
                                 or not 1 <= int(port) <= 65535):
            raise ValueError(error)
    if (len(name) > 255 or not components
            or any(not re.fullmatch(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*", component)
                   for component in components)):
        raise ValueError(error)
    return image


def validate_native_metadata(component):
    """Check retained UPDATE metadata without coercing raw broker values."""
    read = component.get if isinstance(component, dict) else lambda field: getattr(component, field, None)
    for field in ("exposePorts", "isPrivate", "isJob"):
        value = read(field)
        if value is not None and not isinstance(value, bool):
            raise ValueError("Invalid stored native runtime flag")
    for field in ("envVars", "cliArgs"):
        entries = read(field)
        if entries is None:
            continue
        if not isinstance(entries, list):
            raise ValueError("The stored native key/value configuration is invalid")
        for entry in entries:
            key = entry.get("key") if isinstance(entry, dict) else getattr(entry, "key", None)
            value = entry.get("value") if isinstance(entry, dict) else getattr(entry, "value", None)
            if (not isinstance(key, str) or not key or "\x00" in key
                    or (field == "envVars" and "=" in key)
                    or (value is not None and not isinstance(value, str))):
                raise ValueError("The stored native key/value configuration is invalid")


def validate_native_component(component):
    """Validate an effective image workload before native protobuf serialization."""
    validate_native_image_reference(component.containerImage)
    if component.containerResource not in (None, "", component.containerImage):
        raise ValueError("PATCH requires an unambiguous stored native image")
    validate_native_metadata(component)
    if component.isJob:
        raise ValueError("Native Job updates are not supported")

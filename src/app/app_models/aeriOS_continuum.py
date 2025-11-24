""" Module with all aeriOS continumm models in pydantic"""
from typing import List, Literal, Optional
from pydantic import BaseModel


class Area(BaseModel):
    """
    Define the model for the Polygon area
    """
    type: Literal[
        'Polygon'] = 'Polygon'  # Ensures the type is exactly 'Polygon'
    coordinates: List[List[List[
        float]]]  # A list of polygons, which are lists of points (longitude, latitude)


class Organization(BaseModel):
    """
    Pydantic representation of aeriOS Organization
    """
    id: str
    type: Literal["Organization"] = "Organization"
    name: str


class Domain(BaseModel):
    """
    Pydantic representation of aeriOS continuum Domain
    """
    id: str
    type: Literal["Domain"] = "Domain"
    description: Optional[str] = None
    publicUrl: str
    owner: List[str] | Organization  # Organization
    isEntrypoint: bool
    domainStatus: str


class LowLevelOrchestrator(BaseModel):
    """
    Pydantic representation of aeriOS LLO
    """
    id: str
    type: Literal["LowLevelOrchestrator"] = "LowLevelOrchestrator"
    domain: str | Domain
    orchestrationType: str


class InfrastructureElement(BaseModel):
    """
    Pydantic representation of aeriOS continuum IE
    """
    id: str
    type: Literal["InfrastructureElement"] = "InfrastructureElement"
    domain: str | Domain  #Domain
    hostname: str
    containerTechnology: str
    internalIpAddress: str
    macAddress: str
    lowLevelOrchestrator: str | LowLevelOrchestrator  #LowLevelOrchestrator
    cpuCores: int
    currentCpuUsage: int
    ramCapacity: int
    availableRam: int
    currentRamUsage: int
    avgPowerConsumption: int
    currentPowerConsumption: int
    realTimeCapable: bool
    cpuArchitecture: str  # get last part ":"
    operatingSystem: Optional[str] = None  # get last part ":"
    infrastructureElementTier: Optional[str] = None  # get last part ":"
    infrastructureElementStatus: str  # get last part ":"
    location: Optional[Area | dict] = None


class Service(BaseModel):
    """
    Defines the Service model with its attributes
    Example usage when getting back a service entity from CB
        service_instance = Service(**json_data)
    """
    id: str
    type: Literal["Service"] = "Service"
    name: str
    description: str


class NetworkPort(BaseModel):
    """
    aeriOS NetworkPort entity
    """
    id: str
    type: Literal["NetworkPort"] = "NetworkPort"
    portNumber: int
    portProtocol: str


class ServiceComponentStatus(BaseModel):
    """
        "urn:ngsi-ld:ServiceComponentStatus:Running"
        "urn:ngsi-ld:ServiceComponentStatus:Starting"
        "urn:ngsi-ld:ServiceComponentStatus:Overload"
        "urn:ngsi-ld:ServiceComponentStatus:Failed"
        "urn:ngsi-ld:ServiceComponentStatus:Migrating"
        "urn:ngsi-ld:ServiceComponentStatus:Locating"
        "urn:ngsi-ld:ServiceComponentStatus:Removing"
        "urn:ngsi-ld:ServiceComponentStatus:Finished"  
    """
    id: str
    name: str
    description: str


class InfrastructureElementRequirements(BaseModel):
    """
    Define the main model for InfrastructureElementRequirements
    """
    id: str
    type: Literal[
        "InfrastructureElementRequirements"] = "InfrastructureElementRequirements"
    infrastructureElement: Optional[List[str | InfrastructureElement]] = None
    requiredCpuUsage: Optional[int] = None
    requiredRam: Optional[int] = None
    cpuArchitecture: Optional[str] = None
    realTimeCapable: Optional[bool] = None
    area: Optional[Area] = None
    energyEfficiencyRatio: Optional[int] = None
    greenEnergyRatio: Optional[int] = None
    domainId: Optional[str | Domain] = None


class ServiceComponentKeyValue(BaseModel):
    '''
        Added to model cliArgs according to CRD
    '''
    key: str
    value: Optional[str] = None


class ServiceComponent(BaseModel):
    """
    Defines the Service Component model with its attributes
    Exampe usage, when geting back list of service components (in json_data) for service id from CB:
        service_components = [ServiceComponent(**item) for item in json_data]
    """
    id: str
    type: Literal["ServiceComponent"] = "ServiceComponent"
    infrastructureElement: Optional[
        str | InfrastructureElement] = None  # when allocated
    service: str | Service
    serviceComponentStatus: Optional[str | ServiceComponentStatus] = None
    infrastructureElementRequirements: str | InfrastructureElementRequirements
    containerImage: str
    networkPorts: Optional[List[str] | List[NetworkPort]] = None
    envVars: Optional[List[ServiceComponentKeyValue]] = []
    cliArgs: Optional[List[ServiceComponentKeyValue]] = []
    exposePorts: Optional[bool] = None
    isJob: Optional[bool] = None
    isPrivate: Optional[bool] = None
    repoUsername: Optional[str] = None
    repoPassword: Optional[str] = None


#######


class ServiceComponentRequirement(BaseModel):
    """
    Representation of Protobuf message with aeriOS continuum entities
    """
    service_component_definition: ServiceComponent
    infrastructure_element_candidates: List[InfrastructureElement]


class HLOAllocatorOutput(BaseModel):
    """
    Representation of Protobuf output message towards HLO-Allocator
    with aeriOS continuum entities
    """
    service_component_requirements: List[ServiceComponentRequirement]


class ServiceComponentStatusEnum:
    """
    Service Component Status Representation
    """
    RUNNING = "urn:ngsi-ld:ServiceComponentStatus:Running"
    STARTING = "urn:ngsi-ld:ServiceComponentStatus:Starting"
    OVERLOAD = "urn:ngsi-ld:ServiceComponentStatus:Overload"
    FAILED = "urn:ngsi-ld:ServiceComponentStatus:Failed"
    MIGRATING = "urn:ngsi-ld:ServiceComponentStatus:Migrating"
    LOCATING = "urn:ngsi-ld:ServiceComponentStatus:Locating"
    REMOVING = "urn:ngsi-ld:ServiceComponentStatus:Removing"
    FINISHED = "urn:ngsi-ld:ServiceComponentStatus:Finished"

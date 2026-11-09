"""Offline Helm aggregation and initial-placement checks; no app startup or clients."""
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_pipeline():
    """Load the real aggregation/serialization code with external interfaces stubbed."""
    modules = {}
    for name in ("app", "app.clients", "app.services", "app.utils", "app.app_models",
                 "app.app_models.api", "app.app_models.protobuf", "app.app_models.protobuf.generated"):
        module = types.ModuleType(name)
        module.__path__ = [str(ROOT / "src" / name.replace(".", "/"))]
        modules[name] = module
    config = types.ModuleType("app.config")
    config.DEV, config.PRODUCER_TOPIC, config.producer_config = False, "offline", {}
    modules["app.config"] = config
    log = types.ModuleType("app.utils.log")
    log.get_app_logger = lambda: Mock()
    modules["app.utils.log"] = log
    cb = types.ModuleType("app.clients.cb_client")
    cb.CBClient = Mock()
    modules["app.clients.cb_client"] = cb
    continuum = types.ModuleType("app.services.continuum")
    continuum.set_service_failed = Mock()
    continuum.set_service_component_status = Mock()
    modules["app.services.continuum"] = continuum
    kafka = types.ModuleType("confluent_kafka")
    kafka.Producer, kafka.KafkaException = Mock, type("KafkaException", (Exception,), {})
    modules["confluent_kafka"] = kafka
    with patch.dict(sys.modules, modules):
        models = load_file("app.app_models.api.aeros_continuum", ROOT / "src/app/app_models/api/aeros_continuum.py")
        errors = load_file("app.utils.errors", ROOT / "src/app/utils/errors.py")
        serializer = load_file("app.clients.kafka_client", ROOT / "src/app/clients/kafka_client.py")
        ngsild = load_file("app.services.ngsild", ROOT / "src/app/services/ngsild.py")
    return models, serializer, ngsild, errors


MODELS, KAFKA, NGSI, ERRORS = load_pipeline()
STATUS = MODELS.ServiceComponentStatusEnum
VALUES = {"replicaCount": 2, "service": {"ports": [80, 443]}, "enabled": True}


def candidate(identifier="worker-1", technology="Kubernetes", **overrides):
    domain = MODELS.Domain(id="urn:ngsi-ld:Domain:one", publicUrl="https://domain.invalid",
                           owner=[], isEntrypoint=True, domainStatus="Ready")
    llo = MODELS.LowLevelOrchestrator(id="urn:ngsi-ld:LowLevelOrchestrator:one", domain=domain,
                                     orchestrationType="urn:ngsi-ld:OrchestrationType:Kubernetes")
    data = dict(id="urn:ngsi-ld:InfrastructureElement:" + identifier, domain=domain, hostname=identifier,
                containerTechnology=technology, internalIpAddress="10.0.0.1", macAddress="00:00",
                lowLevelOrchestrator=llo, cpuCores=4, currentCpuUsage=5, ramCapacity=1024,
                availableRam=900, currentRamUsage=124, avgPowerConsumption=10,
                currentPowerConsumption=10, realTimeCapable=False, cpuArchitecture="x64",
                infrastructureElementStatus="urn:ngsi-ld:InfrastructureElementStatus:Ready")
    return MODELS.InfrastructureElement(**dict(data, **overrides))


def component(**overrides):
    data = dict(id="urn:ngsi-ld:Service:demo:Component:chart", service="urn:ngsi-ld:Service:demo",
                containerType="HelmChart", containerImage="https://charts.invalid|web:1.2.3",
                containerResource="https://charts.invalid|web:1.2.3", namespace="workloads", timeout="120",
                serviceComponentStatus=STATUS.STARTING,
                infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="urn:ngsi-ld:Requirements:chart"),
                networkPorts=[], envVars=[dict(key="VALUES", value=VALUES)], cliArgs=[],
                exposePorts=False, isJob=False, isPrivate=True, repoUsername="user", repoPassword="secret")
    return MODELS.ServiceComponent(**dict(data, **overrides))


class HelmAggregationTests(unittest.TestCase):
    def test_reads_persisted_helm_metadata(self):
        saved = component(infrastructureElementRequirements="urn:ngsi-ld:Requirements:chart").model_dump()
        client = Mock()
        client.query_entities.return_value = [saved]
        requirements = MODELS.InfrastructureElementRequirements(id=saved["infrastructureElementRequirements"])
        with patch.object(NGSI, "CBClient", return_value=client), patch.object(NGSI, "get_service_components_requirments", return_value=requirements) as get_req:
            loaded = NGSI.get_service_components("urn:ngsi-ld:Service:demo")[0]
        get_req.assert_called_once_with(scomponent_r_id=requirements.id, resolve_ies=False)
        self.assertEqual(loaded.containerResource, saved["containerResource"])
        self.assertEqual((loaded.containerType, loaded.namespace, loaded.timeout), ("HelmChart", "workloads", "120"))
        self.assertEqual(loaded.envVars[0].value, VALUES)
        self.assertEqual(loaded.infrastructureElementRequirements.id, requirements.id)

    def test_helm_values_are_json_once_including_empty_object(self):
        for value in (VALUES, json.dumps(VALUES), {}):
            with self.subTest(value=value):
                serialized = KAFKA.get_service_components(component(envVars=[dict(key="OTHER", value="unchanged"), dict(key="VALUES", value=value)]))
                self.assertEqual(json.loads(serialized.envVars[1].value), {} if value == {} else VALUES)
                self.assertEqual(serialized.envVars[0].value, "unchanged")
                self.assertEqual((serialized.image, serialized.containerType, serialized.namespace, serialized.timeout),
                                 ("https://charts.invalid|web:1.2.3", "HelmChart", "workloads", "120"))
                self.assertEqual((serialized.credentials.username, serialized.credentials.password), ("user", "secret"))

    def test_absent_values_and_metadata_do_not_create_fake_values(self):
        serialized = KAFKA.get_service_components(component(envVars=[], namespace=None, timeout=None, containerResource=None))
        self.assertEqual(list(serialized.envVars), [])
        self.assertEqual((serialized.namespace, serialized.timeout), ("", ""))
        self.assertEqual(serialized.image, "https://charts.invalid|web:1.2.3")

    def test_ordinary_image_fields_ports_jobs_and_plain_values_unchanged(self):
        ordinary = component(containerType=None, containerImage="registry.invalid/web:1", containerResource="unused",
                             namespace=None, timeout=None, envVars=[dict(key="VALUES", value="ordinary"), dict(key="EMPTY", value=None)],
                             cliArgs=[dict(key="--run", value="now")], isJob=True, exposePorts=True,
                             networkPorts=[dict(id="port", portNumber=5353, portProtocol="UDP")])
        serialized = KAFKA.get_service_components(ordinary)
        self.assertEqual(serialized.image, "registry.invalid/web:1")
        self.assertEqual(serialized.containerType, "Docker")
        self.assertEqual([(v.key, v.value) for v in serialized.envVars], [("VALUES", "ordinary"), ("EMPTY", "")])
        self.assertEqual([(v.key, v.value) for v in serialized.cliArgs], [("--run", "now")])
        self.assertTrue(serialized.isJob)
        self.assertTrue(serialized.exposePorts)
        self.assertEqual([(p.number, p.protocol) for p in serialized.ports], [(5353, KAFKA.hlo.UDP)])

    def test_manual_eligible_target_is_preserved_without_discovery(self):
        ie = candidate()
        request = component(infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=[ie.id]))
        with patch.object(NGSI, "get_aeros_ie", return_value=ie), patch.object(NGSI, "CBClient") as client:
            result = NGSI.create_service_component_requirement(request)
        self.assertEqual([v.id for v in result.infrastructure_element_candidates], [ie.id])
        client.assert_not_called()

    def test_unsuitable_manual_target_fails_without_fallback(self):
        ie = candidate(technology="Docker")
        request = component(infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=[ie.id]))
        with patch.object(NGSI, "get_aeros_ie", return_value=ie), patch.object(NGSI, "CBClient") as client:
            self.assertIsNone(NGSI.create_service_component_requirement(request))
        client.assert_not_called()

    def test_restricted_list_filters_in_place_and_preserves_order(self):
        ies = [candidate("docker", "Docker"), candidate("second"), candidate("third")]
        request = component(infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=ies))
        result = NGSI.create_service_component_requirement(request)
        self.assertEqual([v.hostname for v in result.infrastructure_element_candidates], ["second", "third"])

    def test_automatic_candidates_preserve_requirements_and_order(self):
        ies = [candidate("a"), candidate("docker", "Docker"), candidate("b")]
        requirements = MODELS.InfrastructureElementRequirements(id="req", requiredCpuUsage=30, requiredRam=512,
                      cpuArchitecture="x64", realTimeCapable=True, energyEfficiencyRatio=2, domainId="urn:ngsi-ld:Domain:one")
        request = component(infrastructureElementRequirements=requirements)
        client = Mock()
        client.query_entities.return_value = [dict(id=ie.id) for ie in ies]
        with patch.object(NGSI, "CBClient", return_value=client), patch.object(NGSI, "get_aeros_ie", side_effect={ie.id: ie for ie in ies}.get):
            result = NGSI.create_service_component_requirement(request)
        self.assertEqual([v.hostname for v in result.infrastructure_element_candidates], ["a", "b"])
        query = client.query_entities.call_args.kwargs["ngsild_params"]
        for constraint in ('currentCpuUsage<30', 'availableRam>512', 'cpuArchitecture=="x64"', 'realTimeCapable==true', 'energyEfficiencyRatio>=2', 'domain=="urn:ngsi-ld:Domain:one"'):
            self.assertIn(constraint, query)
        self.assertEqual(request.infrastructureElementRequirements, requirements)

    def test_no_eligible_automatic_candidates_fails(self):
        client = Mock()
        client.query_entities.return_value = [dict(id="docker")]
        with patch.object(NGSI, "CBClient", return_value=client), patch.object(NGSI, "get_aeros_ie", return_value=candidate(technology="Docker")):
            self.assertIsNone(NGSI.create_service_component_requirement(component()))

    def test_missing_or_mismatched_routing_is_ineligible(self):
        valid = candidate()
        self.assertTrue(NGSI.is_helm_candidate(valid))
        for invalid in (valid.model_copy(update={"hostname": ""}), valid.model_copy(update={"domain": "unresolved"}),
                        valid.model_copy(update={"lowLevelOrchestrator": "unresolved"}),
                        valid.model_copy(update={"domain": valid.domain.model_copy(update={"publicUrl": ""})}),
                        valid.model_copy(update={"lowLevelOrchestrator": valid.lowLevelOrchestrator.model_copy(update={"domain": "other"})})):
            self.assertFalse(NGSI.is_helm_candidate(invalid))

    def test_missing_candidate_metadata_does_not_escape_existing_failure_path(self):
        request = component(infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=["missing"]))
        with patch.object(NGSI, "get_aeros_ie", side_effect=ERRORS.ResourceNotFoundError("missing")):
            self.assertIsNone(NGSI.create_service_component_requirement(request))

    def test_removal_keeps_workload_type_and_assigned_ie_without_filtering(self):
        ie = candidate(technology="Docker")
        request = component(serviceComponentStatus=STATUS.REMOVING, infrastructureElement=ie)
        with patch.object(NGSI, "get_helm_candidate") as eligibility, patch.object(NGSI, "CBClient") as client:
            result = NGSI.create_service_component_requirement(request)
        eligibility.assert_not_called()
        client.assert_not_called()
        self.assertEqual(result.infrastructure_element_candidates, [])
        serialized = KAFKA.get_service_components(result.service_component_definition)
        self.assertEqual(serialized.containerType, "HelmChart")
        self.assertEqual(serialized.infrastructure_element.id, ie.id)

    def test_ordinary_candidates_are_not_filtered(self):
        ies = [candidate("docker", "Docker"), candidate("k8s")]
        request = component(containerType="Docker", infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=ies))
        with patch.object(NGSI, "get_helm_candidate") as eligibility:
            result = NGSI.create_service_component_requirement(request)
        eligibility.assert_not_called()
        self.assertEqual([v.id for v in result.infrastructure_element_candidates], [v.id for v in ies])

    def test_failed_placement_uses_existing_service_failure_handling(self):
        request = component(infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=[candidate(technology="Docker")]))
        with patch.object(NGSI, "get_service_components", return_value=[request]), patch.object(NGSI.c_utils, "set_service_failed") as failed:
            self.assertIsNone(NGSI.create_hlo_allocator_py(request.service))
        failed.assert_called_once_with(service_id=request.service)

    def test_mixed_service_envelope_preserves_both_workloads(self):
        ie = candidate()
        requirements = MODELS.InfrastructureElementRequirements(id="req", infrastructureElement=[ie])
        helm = component(infrastructureElementRequirements=requirements)
        image = component(id="urn:ngsi-ld:Service:demo:Component:image", containerType="Docker", containerImage="web:1",
                          envVars=[dict(key="MODE", value="prod")], infrastructureElementRequirements=requirements)
        with patch.object(NGSI, "get_service_components", return_value=[helm, image]):
            output = NGSI.create_hlo_allocator_py(helm.service)
        wire = KAFKA.create_hloallocator_output(output)
        restored = KAFKA.aggregator.HLOAllocatorOutput.FromString(wire.SerializeToString())
        definitions = [v.service_component_definition for v in restored.service_component_requirements]
        self.assertEqual([v.containerType for v in definitions], ["HelmChart", "Docker"])
        self.assertEqual([v.image for v in definitions], [helm.containerResource, "web:1"])
        self.assertEqual([v.id for v in definitions], [helm.id, image.id])

    def test_explicit_helm_requirements_preserve_unresolved_target_until_validation(self):
        client = Mock()
        client.query_entity.return_value = dict(id="req", infrastructureElement=[dict(object="missing")])
        with patch.object(NGSI, "CBClient", return_value=client), patch.object(NGSI, "get_aeros_ie") as resolve:
            requirements = NGSI.get_service_components_requirments("req", resolve_ies=False)
        resolve.assert_not_called()
        self.assertEqual(requirements.infrastructureElement, ["missing"])

    def test_scalar_manual_relationship_cannot_fall_back_to_automatic_placement(self):
        for technology in ("Kubernetes", "Docker"):
            with self.subTest(technology=technology):
                ie = candidate(technology=technology)
                client = Mock()
                client.query_entity.return_value = dict(id="req", infrastructureElement=ie.id)
                with patch.object(NGSI, "CBClient", return_value=client):
                    requirements = NGSI.get_service_components_requirments("req", resolve_ies=False)
                with patch.object(NGSI, "get_aeros_ie", return_value=ie), patch.object(NGSI, "CBClient") as discover:
                    result = NGSI.create_service_component_requirement(component(infrastructureElementRequirements=requirements))
                discover.assert_not_called()
                if technology == "Kubernetes":
                    self.assertEqual(result.infrastructure_element_candidates[0].id, ie.id)
                else:
                    self.assertIsNone(result)

    def test_incomplete_domain_owner_metadata_is_ineligible(self):
        with patch.object(NGSI, "get_aeros_ie", side_effect=IndexError("missing owner")):
            self.assertIsNone(NGSI.get_helm_candidate("worker"))


if __name__ == "__main__":
    unittest.main()

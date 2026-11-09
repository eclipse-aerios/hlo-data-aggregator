"""Offline model/wire compatibility checks; run with PYTHONDONTWRITEBYTECODE=1."""
import ast
import importlib.util
import json
from pathlib import Path
import re
import sys
import types
import unittest

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pydantic import ValidationError


ROOT = Path(__file__).resolve().parents[1]
MODEL_ROOT = ROOT / "src/app/app_models"
PROTO_ROOT = MODEL_ROOT / "protobuf"
if not PROTO_ROOT.is_dir():
    PROTO_ROOT = MODEL_ROOT / "schemas"
GENERATED = MODEL_ROOT / "protobuf/generated"
if not GENERATED.is_dir():
    GENERATED = MODEL_ROOT / "py_files"
MODEL_FILE = MODEL_ROOT / "api/aeros_continuum.py"
if not MODEL_FILE.is_file():
    MODEL_FILE = MODEL_ROOT / "aeros_continuum.py"
FIXTURE = json.loads((ROOT / "tests/fixtures/legacy_hlo.json").read_text())
HELM_FIELDS = {"containerType": 13, "namespace": 14, "timeout": 15}
VALUES = {"replicaCount": 2, "enabled": True, "service": {"ports": [80, 443]}}


def load_file(name, path):
    """Load leaf modules without importing the application or its clients."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


MODELS = load_file("contract_continuum_models", MODEL_FILE)
PROTO_PACKAGE = types.ModuleType("contract_proto")
PROTO_PACKAGE.__path__ = [str(GENERATED)]
sys.modules["contract_proto"] = PROTO_PACKAGE
HLO = load_file("contract_proto.hlo_pb2", GENERATED / "hlo_pb2.py")
ENVELOPES = {
    path.stem: load_file(f"contract_proto.{path.stem}", path)
    for path in GENERATED.glob("*_pb2.py") if path.stem != "hlo_pb2"
}
LEGACY_DESCRIPTOR = bytes.fromhex(FIXTURE["descriptors"]["hlo_pb2"])
LEGACY_POOL = descriptor_pool.DescriptorPool()
LEGACY_POOL.AddSerializedFile(LEGACY_DESCRIPTOR)
LEGACY_COMPONENT = message_factory.MessageFactory(LEGACY_POOL).GetPrototype(
    LEGACY_POOL.FindMessageTypeByName("ServiceComponent")
)


def legacy_component_data():
    return {
        "id": "urn:ngsi-ld:Service:legacy:Component:web",
        "service": "urn:ngsi-ld:Service:legacy",
        "infrastructureElementRequirements": "urn:ngsi-ld:Requirements:legacy",
        "containerImage": "registry.example/image:1.0",
        "networkPorts": [{"id": "urn:ngsi-ld:NetworkPort:8080", "type": "NetworkPort",
                          "portNumber": 8080, "portProtocol": "UDP"}],
        "envVars": [{"key": "MODE", "value": "production"}],
        "cliArgs": [{"key": "--debug", "value": "true"}],
        "exposePorts": True, "isJob": True, "isPrivate": True,
        "repoUsername": "legacy-user", "repoPassword": "legacy-password",
    }


def proto_file(data):
    return descriptor_pb2.FileDescriptorProto.FromString(data)


def generated_descriptor(path):
    """Read a sibling artifact without registering its messages in our pool."""
    for node in ast.walk(ast.parse(path.read_text())):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "AddSerializedFile"):
            return proto_file(ast.literal_eval(node.args[0]))
    raise AssertionError(f"Missing generated descriptor in {path}")


def schema_tokens(path):
    source = re.sub(r"//[^\n]*|/\*[\s\S]*?\*/", "", path.read_text())
    return re.findall(r'"[^"\n]*"|[A-Za-z_]\w*|\d+|[^\s]', source)


class ModelCompatibilityTests(unittest.TestCase):
    def test_legacy_image_preserves_flags_credentials_ports_and_strings(self):
        data = legacy_component_data()
        model = MODELS.ServiceComponent(**data)
        dumped = model.model_dump()
        for field in ("containerImage", "networkPorts", "envVars", "cliArgs",
                      "exposePorts", "isJob", "isPrivate", "repoUsername",
                      "repoPassword"):
            self.assertEqual(dumped[field], data[field], field)
        self.assertEqual(model.containerType, "Docker")
        for field in ("containerResource", "namespace", "timeout"):
            self.assertIsNone(getattr(model, field), field)

    def test_helm_metadata_and_nested_values_survive_model_json(self):
        data = legacy_component_data()
        data.update(containerType="HelmChart",
                    containerImage="https://example.invalid/charts|web:1.2.3",
                    containerResource="https://example.invalid/charts|web:1.2.3",
                    namespace="workloads", timeout="120",
                    envVars=[{"key": "VALUES", "value": VALUES}])
        for value in (VALUES, json.dumps(VALUES)):
            with self.subTest(values_type=type(value).__name__):
                data["envVars"][0]["value"] = value
                model = MODELS.ServiceComponent(**data)
                restored = MODELS.ServiceComponent.model_validate_json(model.model_dump_json())
                self.assertEqual(restored.model_dump(), model.model_dump())
                self.assertEqual(restored.envVars[0].value, value)
                self.assertIsInstance(restored.envVars[0].value, type(value))
                self.assertEqual(restored.containerType, "HelmChart")
                self.assertEqual(restored.containerResource, data["containerResource"])
                self.assertEqual((restored.namespace, restored.timeout), ("workloads", "120"))

    def test_optional_metadata_accepts_explicit_none(self):
        model = MODELS.ServiceComponent(**legacy_component_data(), containerType=None,
                                        containerResource=None, namespace=None, timeout=None)
        for field in ("containerType", "containerResource", "namespace", "timeout"):
            self.assertIsNone(getattr(model, field), field)

    def test_container_resource_does_not_replace_required_image_semantics(self):
        data = legacy_component_data()
        del data["containerImage"]
        data.update(containerType="HelmChart", containerResource="charts|web:1")
        with self.assertRaises(ValidationError):
            MODELS.ServiceComponent(**data)

    def test_existing_component_identity_and_requirement_fields_remain_required(self):
        for field in ("id", "service", "infrastructureElementRequirements"):
            with self.subTest(field=field):
                data = legacy_component_data()
                del data[field]
                with self.assertRaises(ValidationError):
                    MODELS.ServiceComponent(**data)
        allocator_model = (PROTO_ROOT / "allocator.proto").is_file()
        self.assertEqual(MODELS.ServiceComponent.model_fields["networkPorts"].is_required(),
                         allocator_model)
        data = legacy_component_data()
        del data["networkPorts"]
        if allocator_model:
            with self.assertRaises(ValidationError):
                MODELS.ServiceComponent(**data)
        else:
            self.assertIsNone(MODELS.ServiceComponent(**data).networkPorts)

    @unittest.skipUnless((ROOT / "src/app/localAllocationManager/models.py").is_file(),
                         "Domain allocation models exist only in Deployment Engine")
    def test_domain_allocation_keeps_nested_ie_and_string_values(self):
        models = load_file("contract_allocation_models",
                           ROOT / "src/app/localAllocationManager/models.py")
        data = {
            "id": "urn:ngsi-ld:Service:legacy:Component:web",
            "image": "registry.example/image:1.0",
            "orchestration_type": "ServiceComponentK8s",
            "infrastructure_element": {"id": "urn:ngsi-ld:IE:worker", "hostname": "worker"},
            "llo_id": "urn:ngsi-ld:LowLevelOrchestrator:legacy",
            "ports": [{"number": 8080, "protocol": "UDP"}],
            "envVars": [{"key": "MODE", "value": "production"}],
            "exposePorts": True, "isJob": True, "isPrivate": True,
            "repoUsername": "legacy-user", "repoPassword": "legacy-password",
        }
        for orchestration_type in ("ServiceComponentK8s", "ServiceComponentDocker"):
            with self.subTest(orchestration_type=orchestration_type):
                legacy_data = dict(data, orchestration_type=orchestration_type)
                legacy = models.ServiceComponentAllocation(**legacy_data)
                self.assertEqual(legacy.model_dump(),
                                 dict(legacy_data, cliArgs=[], namespace=None, timeout=None))
        data.update(image="https://example.invalid/charts|web:1.2.3",
                    orchestration_type="ServiceComponentHelm", namespace="workloads",
                    timeout="120", envVars=[{"key": "VALUES", "value": json.dumps(VALUES)}])
        helm = models.ServiceComponentAllocation(**data)
        self.assertEqual(json.loads(helm.envVars[0].value), VALUES)
        self.assertEqual(helm.infrastructure_element.hostname, "worker")
        self.assertEqual((helm.namespace, helm.timeout), ("workloads", "120"))
        with self.assertRaises(ValidationError):
            models.EnvVars(key="VALUES", value=VALUES)


class ProtobufCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.component = HLO.ServiceComponent.FromString(bytes.fromhex(FIXTURE["component_wire"]))

    def test_legacy_wire_preserves_all_existing_data_and_default_helm_fields(self):
        component = self.component
        self.assertEqual(component.id, "urn:ngsi-ld:Service:legacy:Component:web")
        self.assertEqual(component.image, "registry.example/image:1.0")
        self.assertEqual([(p.number, p.protocol) for p in component.ports], [(8080, HLO.UDP)])
        self.assertEqual(component.service.id, "urn:ngsi-ld:Service:legacy")
        self.assertEqual(dict(component.service_component_constraints.constraints), {"cpu": 32.0})
        self.assertTrue(component.HasField("infrastructure_element"))
        self.assertEqual(component.infrastructure_element.hostname, "worker-1")
        self.assertEqual(component.infrastructure_element.container_technology, "Kubernetes")
        self.assertEqual([(p.key, p.value) for p in component.cliArgs], [("--debug", "true")])
        self.assertEqual([(p.key, p.value) for p in component.envVars], [("MODE", "production")])
        self.assertTrue(component.exposePorts)
        self.assertTrue(component.isJob)
        self.assertTrue(component.is_private)
        self.assertEqual((component.credentials.username, component.credentials.password),
                         ("legacy-user", "legacy-password"))
        for field in HELM_FIELDS:
            self.assertEqual(getattr(component, field), "", field)

    def test_common_descriptor_changes_only_by_the_agreed_three_strings(self):
        baseline = proto_file(LEGACY_DESCRIPTOR)
        current = proto_file(HLO.DESCRIPTOR.serialized_pb)
        message = next(m for m in current.message_type if m.name == "ServiceComponent")
        new_fields = {field.name: field for field in message.field if field.number > 12}
        self.assertEqual(set(new_fields), set(HELM_FIELDS))
        for name, number in HELM_FIELDS.items():
            self.assertEqual((new_fields[name].number, new_fields[name].type,
                              new_fields[name].label), (number, 9, 1))
        preserved_fields = [field for field in message.field if field.number <= 12]
        del message.field[:]
        message.field.extend(preserved_fields)
        self.assertEqual(current, baseline)

    def test_component_source_schema_matches_generated_fields(self):
        source = re.sub(r"//[^\n]*|/\*[\s\S]*?\*/", "", (PROTO_ROOT / "hlo.proto").read_text())
        body = re.search(r"message\s+ServiceComponent\s*\{([^}]*)\}", source).group(1)
        parsed = re.findall(r"\b(?:(optional|repeated)\s+)?(\w+)\s+(\w+)\s*=\s*(\d+)\s*;", body)
        declared = [(name, int(number), field_type, modifier)
                    for modifier, field_type, name, number in parsed]
        component = next(m for m in proto_file(HLO.DESCRIPTOR.serialized_pb).message_type
                         if m.name == "ServiceComponent")
        generated = [(field.name, field.number,
                      field.type_name.lstrip(".") if field.type_name else {8: "bool", 9: "string"}[field.type],
                      "repeated" if field.label == 3 else "optional" if field.proto3_optional else "")
                     for field in component.field]
        self.assertEqual(declared, generated)

    def test_new_fields_roundtrip_and_legacy_reader_keeps_old_meanings(self):
        self.component.containerType = "HelmChart"
        self.component.namespace = "workloads"
        self.component.timeout = "120"
        wire = self.component.SerializeToString()
        self.assertEqual(HLO.ServiceComponent.FromString(wire), self.component)
        old_reader = LEGACY_COMPONENT.FromString(wire)
        self.assertEqual(HLO.ServiceComponent.FromString(old_reader.SerializeToString()),
                         self.component)
        old_reader.DiscardUnknownFields()
        self.assertEqual(old_reader, LEGACY_COMPONENT.FromString(bytes.fromhex(FIXTURE["component_wire"])))

    def test_copy_and_merge_preserve_new_fields_and_existing_credentials(self):
        self.component.containerType = "HelmChart"
        self.component.namespace = "workloads"
        self.component.timeout = "120"
        copied = HLO.ServiceComponent()
        copied.CopyFrom(self.component)
        self.assertEqual(copied, self.component)
        merged = HLO.ServiceComponent()
        merged.MergeFrom(self.component)
        self.assertEqual(merged, self.component)

    def test_existing_envelopes_keep_their_contract_and_carry_new_fields(self):
        self.component.containerType = "HelmChart"
        self.component.namespace = "workloads"
        self.component.timeout = "120"
        component_paths = (
            ("HLOAllocatorOutput", "service_component_requirements", "service_component_definition"),
            ("HLOAllocatorInput", "service_component_requirements", "service_component_definition"),
            ("HLODeploymentEngineInput", "service_component_allocations", "new_allocated_service_component"),
            ("HLODeploymentEngineOutput", "service_component_allocations", "new_allocated_service_component"),
        )
        for name, module in ENVELOPES.items():
            with self.subTest(module=name):
                self.assertEqual(proto_file(module.DESCRIPTOR.serialized_pb),
                                 proto_file(bytes.fromhex(FIXTURE["descriptors"][name])))
            for envelope_name, repeated_field, component_field in component_paths:
                if hasattr(module, envelope_name):
                    envelope_type = getattr(module, envelope_name)
                    envelope = envelope_type()
                    getattr(getattr(envelope, repeated_field).add(), component_field).CopyFrom(self.component)
                    restored = envelope_type.FromString(envelope.SerializeToString())
                    self.assertEqual(getattr(getattr(restored, repeated_field)[0], component_field),
                                     self.component)

    def test_workspace_common_schemas_and_generated_descriptors_agree(self):
        names = ("hlo-fe", "hlo-data-aggregator", "hlo-allocator", "hlo-deployment-engine")
        found = []
        for name in names:
            model_root = ROOT.parent / name / "src/app/app_models"
            schema = model_root / "protobuf/hlo.proto"
            generated = model_root / "protobuf/generated/hlo_pb2.py"
            if not schema.is_file():
                schema = model_root / "schemas/hlo.proto"
                generated = model_root / "py_files/hlo_pb2.py"
            if schema.is_file() and generated.is_file():
                found.append((schema, generated))
        if len(found) < 2:
            self.skipTest("Sibling HLO repositories are not available")
        for schema, generated in found:
            with self.subTest(schema=str(schema)):
                self.assertEqual(schema_tokens(schema), schema_tokens(PROTO_ROOT / "hlo.proto"))
                self.assertEqual(generated_descriptor(generated), proto_file(HLO.DESCRIPTOR.serialized_pb))


if __name__ == "__main__":
    unittest.main()

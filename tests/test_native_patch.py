"""Native UPDATE admission/transport uses existing assignments, never placement."""
import copy
import unittest
from unittest.mock import Mock, patch

from pydantic import ValidationError

from test_helm_deployment import MODELS, KAFKA, NGSI, candidate, component
from test_helm_patch import updating


STATUS = MODELS.ServiceComponentStatusEnum
SERVICE = "urn:ngsi-ld:Service:demo"


def native_update(technology="Docker", **overrides):
    data = dict(
        containerType="Docker", containerImage="registry.invalid:5443/team/app:2",
        containerResource=None, namespace=None, timeout=None,
        envVars=[dict(key="KEEP", value="retained"), dict(key="VALUES", value="ordinary string")],
        cliArgs=[dict(key="--label", value="one"), dict(key="--label", value="two")],
        infrastructureElement=candidate("actual-host", technology), isJob=False)
    data.update(overrides)
    return updating(**data)


def wire_definition(requirement):
    message = KAFKA.create_hloallocator_output([requirement])
    restored = KAFKA.aggregator.HLOAllocatorOutput.FromString(message.SerializeToString())
    return restored.service_component_requirements[0].service_component_definition


def raw_assignment_context():
    """Actual broker references stay scalar until strict UPDATE resolution."""
    ie = candidate("actual-host", "Docker").model_dump()
    domain = ie.pop("domain")
    llo = ie.pop("lowLevelOrchestrator")
    organization = dict(id="urn:ngsi-ld:Organization:owner", type="Organization", name="owner")
    domain["owner"] = [organization["id"]]
    llo["domain"] = domain["id"]
    ie.update(domain=domain["id"], lowLevelOrchestrator=llo["id"])
    return ie, {entity["id"]: entity for entity in (ie, domain, llo, organization)}


class NativePatchAggregationTests(unittest.TestCase):
    def test_native_and_legacy_classes_use_the_actual_docker_or_kubernetes_assignment(self):
        for kind in ("Docker", None, ""):
            for technology in ("Docker", "Kubernetes"):
                with self.subTest(kind=kind, technology=technology):
                    request = native_update(technology, containerType=kind)
                    actual = request.infrastructureElement
                    request.infrastructureElement = actual.id
                    before_requirements = request.infrastructureElementRequirements
                    with patch.object(NGSI, "get_aeros_ie", return_value=actual) as resolve, \
                         patch.object(NGSI, "get_helm_candidate") as eligibility, \
                         patch.object(NGSI, "CBClient") as discovery:
                        result = NGSI.create_service_component_requirement(request)
                    resolve.assert_called_once_with(actual.id, for_update=True)
                    eligibility.assert_not_called()
                    discovery.assert_not_called()
                    self.assertEqual(result.infrastructure_element_candidates, [])
                    self.assertEqual(request.infrastructureElementRequirements, before_requirements)
                    restored = wire_definition(result)
                    self.assertEqual(restored.containerType, "Docker")
                    self.assertEqual(restored.infrastructure_element.id, actual.id)
                    self.assertEqual(restored.infrastructure_element.container_technology, technology)
                    self.assertEqual(restored.infrastructure_element.domain.id, actual.domain.id)
                    self.assertEqual(restored.infrastructure_element.low_level_orchestrator.id,
                                     actual.lowLevelOrchestrator.id)

    def test_raw_legacy_missing_type_is_accepted_without_backfill_or_candidate_discovery(self):
        raw = native_update().model_dump()
        raw.pop("containerType")
        before = copy.deepcopy(raw)
        client = Mock()
        client.query_entities.return_value = [raw]
        with patch.object(NGSI, "CBClient", return_value=client), \
             patch.object(NGSI, "get_service_components_requirments") as requirements:
            result = NGSI.create_hlo_allocator_py(SERVICE)
        requirements.assert_not_called()
        client.query_entity.assert_not_called()
        self.assertEqual(raw, before)
        self.assertEqual(wire_definition(result[0]).containerType, "Docker")

    def test_complete_images_keep_registry_ports_tags_digests_and_ipv6(self):
        for image in ("registry.invalid:5443/team/subteam/app:2",
                      "registry.invalid/team/app@sha256:" + "a" * 64,
                      "[2001:db8::1]:5000/team/app:latest"):
            with self.subTest(image=image):
                request = native_update(containerImage=image, containerResource=image)
                result = NGSI.create_service_component_requirement(request)
                self.assertEqual(wire_definition(result).image, image)

    def test_effective_native_environment_is_string_data_not_helm_values(self):
        request = native_update(envVars=[dict(key="VALUES", value='{"replicas":2}'),
                                        dict(key="EMPTY", value=""), dict(key="FLAG", value=None),
                                        dict(key="BOOL", value="False")])
        before = request.model_dump()
        restored = wire_definition(NGSI.create_service_component_requirement(request))
        self.assertEqual([(v.key, v.value) for v in restored.envVars],
                         [("VALUES", '{"replicas":2}'), ("EMPTY", ""), ("FLAG", ""), ("BOOL", "False")])
        self.assertEqual(request.model_dump(), before)

    def test_cli_order_and_repeated_keys_survive_real_protobuf(self):
        request = native_update(cliArgs=[dict(key="--label", value="first"),
                                        dict(key="--flag", value=""),
                                        dict(key="--label", value="second")])
        restored = wire_definition(NGSI.create_service_component_requirement(request))
        self.assertEqual([(v.key, v.value) for v in restored.cliArgs],
                         [("--label", "first"), ("--flag", ""), ("--label", "second")])

    def test_credentials_ports_and_false_flags_are_preserved_on_the_wire(self):
        port = MODELS.NetworkPort(id="urn:ngsi-ld:NetworkPort:existing", portNumber=8080, portProtocol="TCP")
        request = native_update(networkPorts=[port], exposePorts=False, isPrivate=True,
                                repoUsername="existing-user", repoPassword="existing-password")
        with patch.object(NGSI, "get_aeros_scomponents_ports") as reload_ports:
            restored = wire_definition(NGSI.create_service_component_requirement(request))
        reload_ports.assert_not_called()
        self.assertFalse(restored.isJob)
        self.assertFalse(restored.exposePorts)
        self.assertTrue(restored.is_private)
        self.assertEqual((restored.credentials.username, restored.credentials.password),
                         ("existing-user", "existing-password"))
        self.assertEqual([(p.number, p.protocol) for p in restored.ports], [(8080, KAFKA.hlo.TCP)])
        self.assertEqual(dict(restored.service_component_constraints.constraints), {})

    def test_native_update_ignores_original_placement_requirements_and_ready_filter(self):
        request = native_update(infrastructureElement=candidate("deployed", "Docker", infrastructureElementStatus="NotReady"),
                                infrastructureElementRequirements="stale-reference")
        with patch.object(NGSI, "get_helm_candidate") as initial_eligibility, \
             patch.object(NGSI, "get_service_components_requirments") as requirements, \
             patch.object(NGSI, "CBClient") as discover:
            result = NGSI.create_service_component_requirement(request)
        initial_eligibility.assert_not_called()
        requirements.assert_not_called()
        discover.assert_not_called()
        self.assertEqual(result.infrastructure_element_candidates, [])
        self.assertEqual(result.service_component_definition.infrastructureElement.id,
                         "urn:ngsi-ld:InfrastructureElement:deployed")

    def test_mixed_helm_docker_kubernetes_updates_exclude_running_and_native_noop(self):
        requests = [updating(), native_update(id=SERVICE + ":Component:docker"),
                    native_update("Kubernetes", id=SERVICE + ":Component:k8s")]
        untouched = native_update(id=SERVICE + ":Component:untouched", serviceComponentStatus=STATUS.RUNNING)
        noop = native_update(id=SERVICE + ":Component:noop", serviceComponentStatus=STATUS.RUNNING)
        snapshot = [c.model_dump() for c in (untouched, noop)]
        with patch.object(NGSI, "get_service_components", return_value=requests + [untouched, noop]), \
             patch.object(NGSI, "CBClient") as discover:
            result = NGSI.create_hlo_allocator_py(SERVICE)
        discover.assert_not_called()
        self.assertEqual([r.service_component_definition.id for r in result], [r.id for r in requests])
        self.assertEqual([c.model_dump() for c in (untouched, noop)], snapshot)

    def test_late_invalid_target_uses_existing_batch_failure_policy_without_dispatch(self):
        helm = updating()
        invalid = native_update(id=SERVICE + ":Component:invalid", containerImage="nginx:")
        untouched = native_update(id=SERVICE + ":Component:untouched", serviceComponentStatus=STATUS.RUNNING)
        before = untouched.model_dump()
        with patch.object(NGSI, "get_service_components", return_value=[helm, invalid, untouched]), \
             patch.object(NGSI.c_utils, "set_service_failed") as whole_service, \
             patch.object(NGSI.c_utils, "set_service_component_status") as failed:
            result = NGSI.create_hlo_allocator_py(SERVICE)
        self.assertIsNone(result)
        whole_service.assert_not_called()
        self.assertEqual([call.kwargs["scomponent_id"] for call in failed.call_args_list], [helm.id, invalid.id])
        self.assertEqual(untouched.model_dump(), before)

    def test_unknown_workload_types_are_rejected_before_assignment_resolution(self):
        for kind in ("Kubernetes", "unknown", 0, False):
            with self.subTest(kind=kind):
                request = native_update().model_copy(update={"containerType": kind})
                with patch.object(NGSI, "get_aeros_ie") as resolve:
                    self.assertIsNone(NGSI.create_service_component_requirement(request))
                resolve.assert_not_called()

    def test_incompatible_and_unknown_backend_are_rejected(self):
        for request in (updating(infrastructureElement=candidate("bad", "Docker")),
                        native_update("containerd"), native_update("")):
            with self.subTest(request=request.id):
                self.assertIsNone(NGSI.create_service_component_requirement(request))

    def test_missing_or_redirected_assignment_is_rejected(self):
        for assignment in (None, "", "urn:ngsi-ld:null"):
            with self.subTest(assignment=assignment), patch.object(NGSI, "get_aeros_ie") as resolve:
                self.assertIsNone(NGSI.create_service_component_requirement(native_update(infrastructureElement=assignment)))
                resolve.assert_not_called()
        with patch.object(NGSI, "get_aeros_ie", return_value=candidate("other", "Docker")):
            self.assertIsNone(NGSI.create_service_component_requirement(
                native_update(infrastructureElement="urn:ngsi-ld:InfrastructureElement:actual")))

    def test_missing_mismatched_or_invalid_domain_llo_routing_is_rejected(self):
        actual = candidate("actual", "Docker")
        cases = [actual.model_copy(update={"type": "Other"}),
                 actual.model_copy(update={"hostname": ""}),
                 actual.model_copy(update={"domain": "unresolved"}),
                 actual.model_copy(update={"lowLevelOrchestrator": "unresolved"}),
                 actual.model_copy(update={"domain": actual.domain.model_copy(update={"id": ""})}),
                 actual.model_copy(update={"domain": actual.domain.model_copy(update={"publicUrl": ""})}),
                 actual.model_copy(update={"domain": actual.domain.model_copy(update={"publicUrl": "ftp://domain.invalid"})}),
                 actual.model_copy(update={"lowLevelOrchestrator": actual.lowLevelOrchestrator.model_copy(update={"id": ""})}),
                 actual.model_copy(update={"lowLevelOrchestrator": actual.lowLevelOrchestrator.model_copy(update={"domain": "other"})})]
        for ie in cases:
            with self.subTest(ie=ie.model_dump()):
                self.assertIsNone(NGSI.create_service_component_requirement(native_update(infrastructureElement=ie)))

    def test_wrong_canonical_identity_or_membership_cannot_resolve_assignment(self):
        for fields in ({"id": SERVICE + ":Component:Not_Canonical"},
                       {"service": "urn:ngsi-ld:Service:other"}):
            with self.subTest(fields=fields), patch.object(NGSI, "get_aeros_ie") as resolve:
                self.assertIsNone(NGSI.create_service_component_requirement(native_update(**fields)))
                resolve.assert_not_called()
        other = native_update(id="urn:ngsi-ld:Service:other:Component:app", service="urn:ngsi-ld:Service:other")
        with patch.object(NGSI, "get_service_components", return_value=[other]), \
             patch.object(NGSI, "get_aeros_ie") as resolve, \
             patch.object(NGSI.c_utils, "set_service_component_status"):
            self.assertIsNone(NGSI.create_hlo_allocator_py(SERVICE))
        resolve.assert_not_called()

    def test_invalid_native_images_or_helm_marker_metadata_are_rejected_before_lookup(self):
        for image in ("", "/", ":", "@", "nginx:", "nginx@", "registry.invalid/",
                      "https://registry.invalid/app", "repo|chart:2"):
            with self.subTest(image=image), patch.object(NGSI, "get_aeros_ie") as resolve:
                self.assertIsNone(NGSI.create_service_component_requirement(native_update(containerImage=image)))
                resolve.assert_not_called()
        self.assertIsNone(NGSI.create_service_component_requirement(native_update(containerResource="repo|chart:2")))

    def test_native_jobs_and_non_string_values_cannot_reach_serialization(self):
        for fields in ({"isJob": True},
                       {"envVars": [dict(key="VALUES", value={"replicas": 2})]},
                       {"cliArgs": [dict(key="--value", value={"bad": "shape"})]}):
            with self.subTest(fields=fields), patch.object(NGSI, "get_aeros_ie") as resolve:
                self.assertIsNone(NGSI.create_service_component_requirement(native_update(**fields)))
                resolve.assert_not_called()

    def test_unresolved_linked_port_cannot_silently_disappear_from_update_wire(self):
        with patch.object(NGSI, "get_aeros_scomponents_ports", return_value=[]):
            self.assertIsNone(NGSI.create_service_component_requirement(native_update(networkPorts=["missing-port"])))

    def test_optional_native_collections_have_defined_empty_wire_defaults(self):
        request = native_update(envVars=None, cliArgs=None, exposePorts=None, isPrivate=None)
        restored = wire_definition(NGSI.create_service_component_requirement(request))
        self.assertEqual(list(restored.envVars), [])
        self.assertEqual(list(restored.cliArgs), [])
        self.assertFalse(restored.exposePorts)
        self.assertFalse(restored.is_private)

    def test_raw_false_or_zero_class_is_rejected_without_string_coercion(self):
        for kind in (0, False):
            with self.subTest(kind=kind):
                raw = native_update().model_dump()
                raw["containerType"] = kind
                client = Mock()
                client.query_entities.return_value = [raw]
                with patch.object(NGSI, "CBClient", return_value=client), \
                     patch.object(NGSI, "get_aeros_ie") as resolve, \
                     self.assertRaises(ValidationError):
                    NGSI.get_service_components(SERVICE)
                resolve.assert_not_called()

    def test_raw_update_resolves_exact_references_fresh_without_changing_create_caches(self):
        ie, entities = raw_assignment_context()
        stale = candidate("actual-host", "Kubernetes")
        caches = ({ie["id"]: stale}, {}, {}, {})
        before = [copy.deepcopy(cache) for cache in caches]
        client = Mock()
        client.query_entity.side_effect = lambda entity_id, **kwargs: copy.deepcopy(entities[entity_id])
        with patch.object(NGSI, "CBClient", return_value=client), \
             patch.object(NGSI, "cached_ie", caches[0]), \
             patch.object(NGSI, "cached_domains", caches[1]), \
             patch.object(NGSI, "cached_llo", caches[2]), \
             patch.object(NGSI, "cached_organizations", caches[3]):
            request = native_update(infrastructureElement=ie["id"])
            restored = wire_definition(NGSI.create_service_component_requirement(request))
        self.assertEqual(restored.infrastructure_element.container_technology, "Docker")
        self.assertEqual(restored.infrastructure_element.domain.id, ie["domain"])
        self.assertEqual(restored.infrastructure_element.low_level_orchestrator.id, ie["lowLevelOrchestrator"])
        self.assertEqual(caches, tuple(before))
        self.assertEqual(client.query_entity.call_count, 6)

    def test_wrong_raw_ie_domain_llo_or_owner_response_identity_or_type_is_rejected(self):
        for entity_type in ("InfrastructureElement", "Domain", "LowLevelOrchestrator", "Organization"):
            for field in ("id", "type"):
                with self.subTest(entity_type=entity_type, field=field):
                    ie, entities = raw_assignment_context()
                    assigned_id = ie["id"]
                    entity = next(value for value in entities.values() if value["type"] == entity_type)
                    entity[field] = "redirected"
                    client = Mock()
                    client.query_entity.side_effect = lambda entity_id, **kwargs: copy.deepcopy(entities[entity_id])
                    with patch.object(NGSI, "CBClient", return_value=client), \
                         patch.object(NGSI, "get_helm_candidate") as candidates:
                        self.assertIsNone(NGSI.create_service_component_requirement(
                            native_update(infrastructureElement=assigned_id)))
                    candidates.assert_not_called()

    def test_consistent_but_redirected_nested_domain_and_llo_are_rejected(self):
        ie, entities = raw_assignment_context()
        entities[ie["domain"]]["id"] = "urn:ngsi-ld:Domain:redirected"
        entities[ie["lowLevelOrchestrator"]]["id"] = "urn:ngsi-ld:LowLevelOrchestrator:redirected"
        client = Mock()
        client.query_entity.side_effect = lambda entity_id, **kwargs: copy.deepcopy(entities[entity_id])
        with patch.object(NGSI, "CBClient", return_value=client):
            self.assertIsNone(NGSI.create_service_component_requirement(
                native_update(infrastructureElement=ie["id"])))

    def test_raw_llo_domain_cannot_change_the_assigned_ie_domain(self):
        ie, entities = raw_assignment_context()
        other = copy.deepcopy(entities[ie["domain"]])
        other["id"] = "urn:ngsi-ld:Domain:other"
        entities[other["id"]] = other
        entities[ie["lowLevelOrchestrator"]]["domain"] = other["id"]
        client = Mock()
        client.query_entity.side_effect = lambda entity_id, **kwargs: copy.deepcopy(entities[entity_id])
        with patch.object(NGSI, "CBClient", return_value=client):
            self.assertIsNone(NGSI.create_service_component_requirement(
                native_update(infrastructureElement=ie["id"])))

    def test_default_non_update_resolver_still_uses_existing_cache(self):
        ie = candidate("unchanged", "Docker")
        with patch.object(NGSI, "cached_ie", {ie.id: ie}), patch.object(NGSI, "CBClient") as broker:
            self.assertIs(NGSI.get_aeros_ie(ie.id), ie)
        broker.assert_not_called()

    def test_update_domain_base_url_rejects_invalid_authority_or_appended_uri_traps(self):
        actual = candidate("existing", "Docker")
        for url in ("", "file:///domain", "https://", "https://domain.invalid:", "https://domain.invalid:bad",
                    "https://domain.invalid:70000", "https://domain.invalid:0",
                    "https://host with space.invalid", "https://domain.invalid\x00",
                    "https://domain.invalid\\path", "https://domain.invalid?x=1",
                    "https://domain.invalid#fragment", "https://domain.invalid?", "https://domain.invalid#",
                    "https://user:password@domain.invalid", "https://@domain.invalid"):
            with self.subTest(url=url):
                domain = actual.domain.model_copy(update={"publicUrl": url})
                assigned = actual.model_copy(update={"domain": domain})
                self.assertIsNone(NGSI.create_service_component_requirement(
                    native_update(infrastructureElement=assigned)))
        for url in ("https://domain.invalid", "https://domain.invalid:8443/api/", "http://[::1]:8080/base"):
            with self.subTest(valid_url=url):
                domain = actual.domain.model_copy(update={"publicUrl": url})
                assigned = actual.model_copy(update={"domain": domain})
                self.assertIsNotNone(NGSI.create_service_component_requirement(
                    native_update(infrastructureElement=assigned)))

    def test_raw_retained_native_keys_are_rejected_before_model_construction(self):
        for technology in ("Docker", "Kubernetes"):
            for field, key in (("envVars", "BAD=NAME"), ("envVars", "BAD\x00NAME"),
                               ("cliArgs", "BAD\x00ARG")):
                with self.subTest(technology=technology, field=field, key=key):
                    raw = native_update(technology).model_dump()
                    raw[field] = [dict(key=key, value="retained")]
                    before = copy.deepcopy(raw)
                    client = Mock()
                    client.query_entities.return_value = [raw]
                    with patch.object(NGSI, "CBClient", return_value=client), \
                         patch.object(NGSI.aeros_C, "ServiceComponent") as constructor, \
                         patch.object(NGSI, "get_aeros_ie") as assignment:
                        with self.assertRaises(NGSI.NativeUpdateMetadataError) as caught:
                            NGSI.get_service_components(SERVICE)
                    self.assertEqual(caught.exception.component_ids, [raw["id"]])
                    constructor.assert_not_called()
                    assignment.assert_not_called()
                    self.assertEqual(raw, before)

    def test_raw_string_and_integer_runtime_flags_are_not_coerced_for_native_updates(self):
        for field in ("exposePorts", "isPrivate", "isJob"):
            for value in ("false", "true", 0, 1):
                with self.subTest(field=field, value=value):
                    raw = native_update().model_dump()
                    raw[field] = value
                    client = Mock()
                    client.query_entities.return_value = [raw]
                    with patch.object(NGSI, "CBClient", return_value=client), \
                         patch.object(NGSI.aeros_C, "ServiceComponent") as constructor:
                        with self.assertRaises(NGSI.NativeUpdateMetadataError):
                            NGSI.get_service_components(SERVICE)
                    constructor.assert_not_called()
                    self.assertIs(raw[field], value)

    def test_invalid_raw_native_metadata_retains_existing_updating_target_failure_policy(self):
        helm = updating().model_dump()
        valid = native_update(id=SERVICE + ":Component:valid").model_dump()
        invalid = native_update(id=SERVICE + ":Component:invalid").model_dump()
        invalid["isPrivate"] = "false"
        untouched = native_update(id=SERVICE + ":Component:untouched",
                                  serviceComponentStatus=STATUS.RUNNING).model_dump()
        raw = [helm, valid, invalid, untouched]
        before = copy.deepcopy(raw)
        client = Mock()
        client.query_entities.return_value = raw
        with patch.object(NGSI, "CBClient", return_value=client), \
             patch.object(NGSI, "create_service_component_requirement") as requirement, \
             patch.object(NGSI.c_utils, "set_service_failed") as whole_service, \
             patch.object(NGSI.c_utils, "set_service_component_status") as failed:
            self.assertIsNone(NGSI.create_hlo_allocator_py(SERVICE))
        requirement.assert_not_called()
        client.query_entity.assert_not_called()
        whole_service.assert_not_called()
        self.assertEqual([c.kwargs["scomponent_id"] for c in failed.call_args_list],
                         [helm["id"], valid["id"], invalid["id"]])
        self.assertTrue(all(c.kwargs["scomponent_status"] == STATUS.FAILED
                            for c in failed.call_args_list))
        self.assertEqual(raw, before)

    def test_direct_modeled_native_validation_matches_raw_key_and_flag_policy(self):
        changes = [{"envVars": [dict(key="BAD=NAME", value="stored")]},
                   {"envVars": [dict(key="BAD\x00NAME", value="stored")]},
                   {"cliArgs": [dict(key="--bad\x00arg", value="stored")]}]
        changes.extend({field: value} for field in ("exposePorts", "isPrivate", "isJob")
                       for value in ("false", 0))
        for fields in changes:
            with self.subTest(fields=fields), patch.object(NGSI, "get_aeros_ie") as assignment:
                request = native_update().model_copy(update=fields)
                self.assertIsNone(NGSI.create_service_component_requirement(request))
                assignment.assert_not_called()

    def test_valid_boolean_none_metadata_and_legacy_types_reach_real_wire_unchanged(self):
        for technology in ("Docker", "Kubernetes"):
            for expose in (True, False, None):
                for private in (True, False, None):
                    for job in (False, None):
                        with self.subTest(technology=technology, expose=expose, private=private, job=job):
                            raw = native_update(technology, exposePorts=expose, isPrivate=private,
                                                isJob=job).model_dump()
                            raw.pop("containerType")
                            before = copy.deepcopy(raw)
                            client = Mock()
                            client.query_entities.return_value = [raw]
                            with patch.object(NGSI, "CBClient", return_value=client):
                                result = NGSI.create_hlo_allocator_py(SERVICE)
                            restored = wire_definition(result[0])
                            self.assertEqual(restored.containerType, "Docker")
                            self.assertEqual(restored.exposePorts, bool(expose))
                            self.assertEqual(restored.is_private, bool(private))
                            self.assertFalse(restored.isJob)
                            self.assertEqual(raw, before)

    def test_retained_cli_equals_order_and_repeated_keys_remain_supported(self):
        raw = native_update(cliArgs=[dict(key="--set=value", value="one"),
                                     dict(key="--set=value", value="two"),
                                     dict(key="--empty=", value=None)]).model_dump()
        before = copy.deepcopy(raw)
        client = Mock()
        client.query_entities.return_value = [raw]
        with patch.object(NGSI, "CBClient", return_value=client):
            result = NGSI.create_hlo_allocator_py(SERVICE)
        restored = wire_definition(result[0])
        self.assertEqual([(v.key, v.value) for v in restored.cliArgs],
                         [("--set=value", "one"), ("--set=value", "two"), ("--empty=", "")])
        self.assertEqual(raw, before)

    def test_raw_native_metadata_gate_does_not_change_non_update_readers(self):
        for state in (STATUS.STARTING, STATUS.REMOVING, STATUS.RUNNING):
            with self.subTest(state=state):
                raw = native_update(serviceComponentStatus=state).model_dump()
                raw["isPrivate"] = "false"
                raw["envVars"] = [dict(key="LEGACY=NAME", value="stored")]
                before = copy.deepcopy(raw)
                client = Mock()
                client.query_entities.return_value = [raw]
                requirement = MODELS.InfrastructureElementRequirements(id="original")
                with patch.object(NGSI, "CBClient", return_value=client), \
                     patch.object(NGSI, "get_aeros_scomponents_ports", return_value=[]), \
                     patch.object(NGSI, "get_service_components_requirments", return_value=requirement):
                    loaded = NGSI.get_service_components(SERVICE)[0]
                self.assertFalse(loaded.isPrivate)
                self.assertEqual(loaded.envVars[0].key, "LEGACY=NAME")
                self.assertEqual(raw, before)

    def test_update_metadata_gate_does_not_validate_untouched_running_metadata(self):
        target = native_update().model_dump()
        untouched = native_update(id=SERVICE + ":Component:untouched",
                                  serviceComponentStatus=STATUS.RUNNING).model_dump()
        untouched.update(isPrivate="false", envVars=[dict(key="LEGACY=NAME", value="stored")])
        before = copy.deepcopy([target, untouched])
        client = Mock()
        client.query_entities.return_value = [target, untouched]
        with patch.object(NGSI, "CBClient", return_value=client):
            result = NGSI.create_hlo_allocator_py(SERVICE)
        self.assertEqual([r.service_component_definition.id for r in result], [target["id"]])
        self.assertEqual([target, untouched], before)


if __name__ == "__main__":
    unittest.main()

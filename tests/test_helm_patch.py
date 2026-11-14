"""Offline update aggregation checks using actual broker-model/protobuf code."""
import copy
import json
import unittest
from unittest.mock import Mock, patch

from test_helm_deployment import MODELS, KAFKA, NGSI, ERRORS, candidate, component, VALUES


STATUS = MODELS.ServiceComponentStatusEnum


def updating(**overrides):
    data = dict(serviceComponentStatus=STATUS.UPDATING,
                infrastructureElement=candidate("actual-host"),
                infrastructureElementRequirements="urn:ngsi-ld:Requirements:original")
    data.update(overrides)
    return component(**data)


class HelmPatchAggregationTests(unittest.TestCase):
    def test_current_assignment_wins_over_original_manual_candidate(self):
        actual = candidate("actual-host")
        previous = candidate("original-candidate")
        request = updating(infrastructureElement=actual.id,
                           infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(
                               id="req", infrastructureElement=[previous]))
        with patch.object(NGSI, "get_aeros_ie", return_value=actual) as resolve, \
             patch.object(NGSI, "get_helm_candidate") as eligibility, \
             patch.object(NGSI, "CBClient") as discovery:
            result = NGSI.create_service_component_requirement(request)
        resolve.assert_called_once_with(actual.id, for_update=True)
        eligibility.assert_not_called()
        discovery.assert_not_called()
        self.assertEqual(result.infrastructure_element_candidates, [])
        self.assertEqual(result.service_component_definition.infrastructureElement, actual)
        self.assertEqual(request.infrastructureElementRequirements.infrastructureElement, [previous])

    def test_original_automatic_requirements_need_no_candidate_discovery(self):
        actual = candidate("automatically-assigned")
        request = updating(infrastructureElement=actual,
                           infrastructureElementRequirements=MODELS.InfrastructureElementRequirements(
                               id="req", infrastructureElement=None, requiredRam=100000))
        with patch.object(NGSI, "CBClient") as discovery, \
             patch.object(NGSI, "get_helm_candidate") as eligibility:
            requirement = NGSI.create_service_component_requirement(request)
        discovery.assert_not_called()
        eligibility.assert_not_called()
        self.assertEqual(requirement.service_component_definition.infrastructureElement.id, actual.id)
        self.assertEqual(requirement.infrastructure_element_candidates, [])

    def test_update_does_not_apply_initial_placement_eligibility(self):
        actual = candidate("existing", infrastructureElementStatus="NotReady")
        request = updating(infrastructureElement=actual)
        with patch.object(NGSI, "is_helm_candidate", return_value=False) as eligibility:
            result = NGSI.create_service_component_requirement(request)
        eligibility.assert_not_called()
        self.assertIsNotNone(result)
        self.assertEqual(result.service_component_definition.infrastructureElement.id, actual.id)

    def test_get_components_reads_update_without_resolving_original_requirements(self):
        saved = updating(infrastructureElement=candidate().id).model_dump()
        untouched = component(id="urn:ngsi-ld:Service:demo:Component:image", containerType="Docker",
                              serviceComponentStatus=STATUS.RUNNING, infrastructureElement=None,
                              infrastructureElementRequirements="stale-reference").model_dump()
        broker = [saved, untouched]
        expected = copy.deepcopy(broker)
        client = Mock()
        client.query_entities.return_value = broker
        with patch.object(NGSI, "CBClient", return_value=client), \
             patch.object(NGSI, "get_service_components_requirments") as requirements, \
             patch.object(NGSI, "get_aeros_ie") as resolution:
            result = NGSI.get_service_components("urn:ngsi-ld:Service:demo")
        requirements.assert_not_called()
        resolution.assert_not_called()
        self.assertEqual(result[0].infrastructureElement, saved["infrastructureElement"])
        self.assertEqual(result[1].serviceComponentStatus, STATUS.RUNNING)
        self.assertIsNone(result[1].infrastructureElement)
        self.assertEqual(broker, expected)

    def test_update_metadata_and_assigned_ie_survive_real_wire(self):
        actual = candidate("assigned")
        request = updating(infrastructureElement=actual, containerResource="oci://registry.invalid/charts|web:2.0",
                           namespace="updated", timeout="240", envVars=[dict(key="VALUES", value=VALUES)])
        result = NGSI.create_service_component_requirement(request)
        wire = KAFKA.create_hloallocator_output([result]).SerializeToString()
        restored = KAFKA.aggregator.HLOAllocatorOutput.FromString(wire)
        update = restored.service_component_requirements[0].service_component_definition
        self.assertEqual(update.infrastructure_element.id, actual.id)
        self.assertEqual(update.infrastructure_element.domain.id, actual.domain.id)
        self.assertEqual(update.infrastructure_element.low_level_orchestrator.id, actual.lowLevelOrchestrator.id)
        self.assertEqual((update.containerType, update.image, update.namespace, update.timeout),
                         ("HelmChart", "oci://registry.invalid/charts|web:2.0", "updated", "240"))
        self.assertEqual(json.loads(update.envVars[0].value), VALUES)
        self.assertTrue(update.is_private)
        self.assertEqual((update.credentials.username, update.credentials.password), ("user", "secret"))
        self.assertEqual(dict(update.service_component_constraints.constraints), {})
        self.assertEqual(list(restored.service_component_requirements[0].infrastructure_element_candidates), [])

    def test_empty_values_and_optional_nulls_have_defined_wire_defaults(self):
        request = updating(envVars=[dict(key="VALUES", value={})], cliArgs=None,
                           namespace=None, timeout=None, isJob=None, exposePorts=None)
        result = KAFKA.get_service_components(request)
        self.assertEqual(result.envVars[0].value, "{}")
        self.assertEqual((result.namespace, result.timeout), ("", ""))
        self.assertFalse(result.isJob)
        self.assertFalse(result.exposePorts)

    def test_subset_keeps_untouched_running_component_nonactionable(self):
        update = updating()
        untouched = component(id="urn:ngsi-ld:Service:demo:Component:image", containerType="Docker",
                              serviceComponentStatus=STATUS.RUNNING, infrastructureElement="stale-assignment",
                              infrastructureElementRequirements="stale-requirements")
        before = untouched.model_dump()
        with patch.object(NGSI, "get_service_components", return_value=[update, untouched]), \
             patch.object(NGSI, "CBClient") as discovery:
            requirements = NGSI.create_hlo_allocator_py(update.service)
        discovery.assert_not_called()
        self.assertEqual(untouched.model_dump(), before)
        wire = KAFKA.create_hloallocator_output(requirements)
        self.assertEqual([r.service_component_definition.id for r in wire.service_component_requirements],
                         [update.id])
        self.assertEqual(wire.service_component_requirements[0].service_component_definition.infrastructure_element.id,
                         update.infrastructureElement.id)

    def test_untouched_failed_component_does_not_retry_during_subset_update(self):
        update = updating().model_dump()
        untouched = component(id="urn:ngsi-ld:Service:demo:Component:failed", serviceComponentStatus=STATUS.FAILED,
                              infrastructureElement="original-assignment", networkPorts=["stale-port"],
                              infrastructureElementRequirements="stale-requirements").model_dump()
        broker = [update, untouched]
        before = copy.deepcopy(broker)
        client = Mock()
        client.query_entities.return_value = broker
        with patch.object(NGSI, "CBClient", return_value=client), \
             patch.object(NGSI, "get_service_components_requirments") as requirements, \
             patch.object(NGSI, "get_aeros_scomponents_ports") as ports:
            result = NGSI.create_hlo_allocator_py(update["service"])
        requirements.assert_not_called()
        ports.assert_not_called()
        client.query_entity.assert_not_called()
        self.assertEqual([r.service_component_definition.id for r in result], [update["id"]])
        self.assertEqual(broker, before)

    def test_failed_retry_without_updates_keeps_existing_discovery(self):
        failed = component(serviceComponentStatus=STATUS.FAILED,
                           infrastructureElementRequirements="original-requirements").model_dump()
        assigned = candidate("retry-candidate")
        client = Mock()
        client.query_entities.side_effect = [[failed], [{"id": assigned.id}]]
        requirements = MODELS.InfrastructureElementRequirements(id="original-requirements", requiredRam=512)
        with patch.object(NGSI, "CBClient", return_value=client), \
             patch.object(NGSI, "get_service_components_requirments", return_value=requirements) as load_requirements, \
             patch.object(NGSI, "get_aeros_ie", return_value=assigned):
            result = NGSI.create_hlo_allocator_py(failed["service"])
        load_requirements.assert_called_once_with(scomponent_r_id="original-requirements", resolve_ies=True)
        self.assertEqual(result[0].infrastructure_element_candidates, [assigned])
        self.assertIn("availableRam>512", client.query_entities.call_args.kwargs["ngsild_params"])

    def test_missing_assignment_cannot_fall_back_to_original_requirements(self):
        for assignment in (None, "", "urn:ngsi-ld:null"):
            with self.subTest(assignment=assignment), patch.object(NGSI, "get_aeros_ie") as resolve, \
                 patch.object(NGSI, "CBClient") as discovery:
                self.assertIsNone(NGSI.create_service_component_requirement(updating(infrastructureElement=assignment)))
                resolve.assert_not_called()
                discovery.assert_not_called()

    def test_failed_assignment_resolution_uses_targeted_failure(self):
        update = updating(infrastructureElement="missing-assigned-ie")
        untouched = component(id="urn:ngsi-ld:Service:demo:Component:untouched", serviceComponentStatus=STATUS.RUNNING)
        before = untouched.model_dump()
        with patch.object(NGSI, "get_service_components", return_value=[update, untouched]), \
             patch.object(NGSI, "get_aeros_ie", side_effect=ERRORS.ResourceNotFoundError("missing")), \
             patch.object(NGSI.c_utils, "set_service_failed") as all_failed, \
             patch.object(NGSI.c_utils, "set_service_component_status") as failed:
            self.assertIsNone(NGSI.create_hlo_allocator_py(update.service))
        all_failed.assert_not_called()
        failed.assert_called_once_with(scomponent_id=update.id, scomponent_status=STATUS.FAILED)
        self.assertEqual(untouched.model_dump(), before)

    def test_assignment_resolution_cannot_redirect_update(self):
        with patch.object(NGSI, "get_aeros_ie", return_value=candidate("different-host")):
            self.assertIsNone(NGSI.create_service_component_requirement(
                updating(infrastructureElement="urn:ngsi-ld:InfrastructureElement:current")))

    def test_update_port_broker_failure_marks_only_targets(self):
        update = updating(networkPorts=["unavailable-port"])
        untouched = component(id="urn:ngsi-ld:Service:demo:Component:untouched", serviceComponentStatus=STATUS.RUNNING)
        before = untouched.model_dump()
        with patch.object(NGSI, "get_service_components", return_value=[update, untouched]), \
             patch.object(NGSI, "get_aeros_scomponents_ports", side_effect=ERRORS.ServiceUnavailableError("broker unavailable")), \
             patch.object(NGSI.c_utils, "set_service_failed") as all_failed, \
             patch.object(NGSI.c_utils, "set_service_component_status") as failed:
            self.assertIsNone(NGSI.create_hlo_allocator_py(update.service))
        all_failed.assert_not_called()
        failed.assert_called_once_with(scomponent_id=update.id, scomponent_status=STATUS.FAILED)
        self.assertEqual(untouched.model_dump(), before)

    def test_nonhelm_update_is_rejected_without_selection(self):
        with patch.object(NGSI, "get_aeros_ie") as resolve, patch.object(NGSI, "CBClient") as discovery:
            self.assertIsNone(NGSI.create_service_component_requirement(updating(containerType="Docker")))
        resolve.assert_not_called()
        discovery.assert_not_called()

    def test_targeted_failure_write_error_does_not_trigger_all_service_rollback(self):
        update = updating(infrastructureElement=None)
        untouched = component(id="urn:ngsi-ld:Service:demo:Component:untouched", serviceComponentStatus=STATUS.RUNNING)
        before = untouched.model_dump()
        with patch.object(NGSI, "get_service_components", return_value=[update, untouched]), \
             patch.object(NGSI.c_utils, "set_service_failed") as all_failed, \
             patch.object(NGSI.c_utils, "set_service_component_status", side_effect=ERRORS.ServiceUnavailableError("broker write failed")):
            self.assertIsNone(NGSI.create_hlo_allocator_py(update.service))
        all_failed.assert_not_called()
        self.assertEqual(untouched.model_dump(), before)


if __name__ == "__main__":
    unittest.main()

"""Read-boundary null placement handling without losing explicit selections."""
import copy
import unittest
from unittest.mock import Mock, patch

import test_helm_deployment as deployment


class HelmPlacementNormalizationTests(unittest.TestCase):
    def read_requirements(self, raw, resolve_ies):
        client = Mock()
        client.query_entity.return_value = dict(id='req', infrastructureElement=copy.deepcopy(raw))
        with patch.object(deployment.NGSI, 'CBClient', return_value=client), \
                patch.object(deployment.NGSI, 'get_aeros_ie') as resolve:
            result = deployment.NGSI.get_service_components_requirments('req', resolve_ies=resolve_ies)
        resolve.assert_not_called()
        return result.infrastructureElement

    def test_null_placement_is_normalized_even_when_helm_defers_resolution(self):
        null = 'urn:ngsi-ld:null'
        for raw in (null, [null], {'type': 'Relationship', 'object': null},
                    [{'type': 'Relationship', 'object': null}]):
            with self.subTest(representation=raw):
                self.assertIsNone(self.read_requirements(raw, resolve_ies=False))

    def test_ordinary_null_placement_keeps_automatic_selection_semantics(self):
        null = 'urn:ngsi-ld:null'
        for raw in ([null], {'type': 'Relationship', 'object': null},
                    [{'type': 'Relationship', 'object': null}]):
            with self.subTest(representation=raw):
                self.assertIsNone(self.read_requirements(raw, resolve_ies=True))

    def test_other_explicit_lists_are_preserved_for_validation_in_order(self):
        for raw in (['missing'], ['second', 'first'], ['urn:ngsi-ld:null', 'real-ie']):
            with self.subTest(targets=raw):
                self.assertEqual(self.read_requirements(raw, resolve_ies=False), raw)


if __name__ == '__main__':
    unittest.main()

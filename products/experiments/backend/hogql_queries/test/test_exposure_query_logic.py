import pytest

from posthog.schema import (
    ActionsNode,
    ExperimentEventExposureConfig,
    ExperimentExposureCriteria,
    MultipleVariantHandling,
)

from posthog.models.team import Team

from products.experiments.backend.hogql_queries.exposure_query_logic import (
    get_exposure_event_and_property,
    get_multiple_variant_handling_from_experiment,
    get_test_accounts_filter,
    normalize_to_exposure_criteria,
)


class TestNormalizeToExposureCriteria:
    @pytest.mark.parametrize(
        "input_value,expected_type",
        [
            (None, type(None)),
            (ExperimentExposureCriteria(), ExperimentExposureCriteria),
            ({}, ExperimentExposureCriteria),
            ({"exposure_config": {"event": "test", "properties": []}}, ExperimentExposureCriteria),
        ],
    )
    def test_handles_different_input_types(self, input_value, expected_type):
        result = normalize_to_exposure_criteria(input_value)
        assert isinstance(result, expected_type)

    def test_does_not_mutate_input_dict(self):
        original = {"exposure_config": {"event": "test", "properties": []}}
        original_copy = original.copy()

        normalize_to_exposure_criteria(original)

        # Original dict should remain unchanged
        assert original == original_copy
        assert isinstance(original["exposure_config"], dict)

    def test_converts_nested_exposure_config(self):
        input_dict = {"exposure_config": {"event": "test_event", "properties": []}}

        result = normalize_to_exposure_criteria(input_dict)

        assert result is not None
        assert isinstance(result.exposure_config, ExperimentEventExposureConfig)
        assert result.exposure_config.event == "test_event"

    def test_preserves_already_typed_object(self):
        typed_criteria = ExperimentExposureCriteria()

        result = normalize_to_exposure_criteria(typed_criteria)

        # Should return the exact same object, not a copy
        assert result is typed_criteria


class TestGetExposureEventAndProperty:
    # The event/variant-property pairing is the contract every exposure consumer builds on
    # (analysis, replay session context, the scanning-experiments-with-replay-vision skill).
    @pytest.mark.parametrize(
        "exposure_criteria,expected",
        [
            (None, ("$feature_flag_called", "$feature_flag_response")),
            (
                {"exposure_config": {"event": "$feature_flag_called", "properties": []}},
                ("$feature_flag_called", "$feature_flag_response"),
            ),
            (
                {"exposure_config": {"event": "checkout completed", "properties": []}},
                ("checkout completed", "$feature/my-flag"),
            ),
            (
                ExperimentExposureCriteria(exposure_config=ActionsNode(id=42)),
                (None, "$feature/my-flag"),
            ),
        ],
    )
    def test_maps_exposure_config_to_event_and_variant_property(self, exposure_criteria, expected):
        assert get_exposure_event_and_property("my-flag", exposure_criteria) == expected


class TestGetTestAccountsFilter:
    _team_filter = {"key": "$host", "type": "event", "value": "localhost", "operator": "not_icontains"}

    @pytest.mark.parametrize("exposure_criteria", [None, {}, {"filterTestAccounts": False}])
    def test_does_not_filter_unless_opted_in(self, exposure_criteria):
        # filterTestAccounts defaults to False: absent criteria must not pick up the team's filters.
        team = Team(id=1, project_id=1, test_account_filters=[self._team_filter])
        assert get_test_accounts_filter(team, exposure_criteria) == []

    def test_applies_team_filters_when_opted_in(self):
        team = Team(id=1, project_id=1, test_account_filters=[self._team_filter])
        assert len(get_test_accounts_filter(team, {"filterTestAccounts": True})) == 1


class TestGetMultipleVariantHandling:
    @pytest.mark.parametrize(
        "exposure_criteria,expected",
        [
            (None, MultipleVariantHandling.EXCLUDE),
            ({}, MultipleVariantHandling.EXCLUDE),
            ({"multiple_variant_handling": "first_seen"}, MultipleVariantHandling.FIRST_SEEN),
        ],
    )
    def test_defaults_to_exclude(self, exposure_criteria, expected):
        assert get_multiple_variant_handling_from_experiment(exposure_criteria) == expected

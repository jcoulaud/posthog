"""Read/write helpers for per-(Slack workspace, Slack user) settings backed
by `models.SlackSettings`. Currently exposes AI-preference resolution; future
per-user / per-workspace knobs belong here too.

Key names mirror the task-run request serializer
(`products/tasks/backend/presentation/serializers.py`) so the resolver output
can be handed to the task layer with zero translation.

Only the per-user row carries AI preferences. A workspace-wide model is a
project-level decision and lives in `TeamTasksConfig`, reachable from PostHog
settings: a Slack workspace can route to several PostHog projects, so a
workspace-keyed default can't say "Opus in project A, Sonnet in project B"
even though every run lands in exactly one project.

Resolution is a whole-triple swap, never a field-by-field merge — the row
either sources the atomic `(runtime_adapter, model)` pair or contributes
nothing. `reasoning_effort` is dropped if the model doesn't support it, so a
stale effort from a previous model choice can't silently stick. Unset keys
stay `None` so the task layer applies its own defaults rather than
duplicating them here.

Gated by the `slack-app-home` feature flag: when off the resolver returns
the empty object, preserving pre-Home-tab behaviour for workspaces that
haven't opted in.

Layering with the tasks product's central defaults: the resolved triple is
passed to task creation as explicit per-run values, so a Slack user's own pick
sits above the central per-user / per-team defaults (see
`products.tasks.backend.facade.ai_run_defaults`) — the same rule PostHog Code
applies to its device-local pick. With no personal row the task layer resolves
those central defaults on its own.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from products.slack_app.backend.feature_flags import is_slack_app_home_enabled

if TYPE_CHECKING:
    from posthog.models.integration import Integration


@dataclass(frozen=True)
class AIPreferences:
    """Resolved AI preferences for a single (workspace, slack_user_id) lookup.

    Field names match the task-run request serializer so callers can splat this
    straight into the task creation payload.
    """

    runtime_adapter: str | None = None
    model: str | None = None
    reasoning_effort: str | None = None

    @property
    def is_empty(self) -> bool:
        return self.runtime_adapter is None and self.model is None and self.reasoning_effort is None


_EMPTY = AIPreferences()


def resolve_ai_preferences(integration: Integration, slack_user_id: str | None) -> AIPreferences:
    """Resolve this Slack user's own AI preference for a workspace.

    Empty unless the user's row carries the atomic `(runtime_adapter, model)`
    pair; an empty result hands the decision to the tasks layer's project and
    user defaults. `reasoning_effort` is dropped if the model doesn't support it.
    """

    if not is_slack_app_home_enabled(integration) or not slack_user_id:
        return _EMPTY

    from products.slack_app.backend.models import SlackSettings

    # Pulled into a local dict so mypy can give it a definite type — the
    # JSONField returns `Any | None`, and a `... or {}` expression ends up as a
    # wider union mypy refuses to narrow.
    stored = (
        SlackSettings.objects.filter(slack_workspace_id=integration.integration_id, slack_user_id=slack_user_id)
        .values_list("ai_preferences", flat=True)
        .first()
    )
    user_prefs: dict[str, Any] = stored or {}

    # `validate_ai_preferences` enforces that `runtime_adapter` and `model` are
    # set together, so the presence of either one is a faithful signal that this
    # row has been explicitly configured.
    chosen = user_prefs if user_prefs.get("runtime_adapter") and user_prefs.get("model") else {}

    runtime_adapter = chosen.get("runtime_adapter") or None
    model = chosen.get("model") or None
    reasoning_effort = chosen.get("reasoning_effort") or None
    if runtime_adapter and model and reasoning_effort:
        reasoning_effort = _filter_unsupported_effort(runtime_adapter, model, reasoning_effort)

    return AIPreferences(
        runtime_adapter=runtime_adapter,
        model=model,
        reasoning_effort=reasoning_effort,
    )


# The Slack row stores the same triple as `UserTasksConfig`, so validation and packing
# are the tasks product's rules — delegated rather than restated, or the two copies drift
# every time the gateway gains an adapter or effort tier. Imported inside the functions to
# keep the tasks stack off the slack_app import path, as elsewhere in this module.


def _filter_unsupported_effort(runtime_adapter: str, model: str, effort: str) -> str | None:
    from products.tasks.backend.facade.ai_run_defaults import filter_unsupported_effort  # noqa: PLC0415

    return filter_unsupported_effort(runtime_adapter, model, effort)


def build_ai_preferences_payload(
    runtime_adapter: str | None, model: str | None, reasoning_effort: str | None
) -> dict[str, str]:
    from products.tasks.backend.facade.ai_run_defaults import build_ai_run_preferences_payload  # noqa: PLC0415

    return build_ai_run_preferences_payload(runtime_adapter, model, reasoning_effort)


def validate_ai_preferences(runtime_adapter: str | None, model: str | None, reasoning_effort: str | None) -> None:
    """Raises `django.core.exceptions.ValidationError` if the triple is inconsistent."""
    from products.tasks.backend.facade.ai_run_defaults import validate_ai_run_preferences  # noqa: PLC0415

    validate_ai_run_preferences(runtime_adapter, model, reasoning_effort)


__all__ = [
    "AIPreferences",
    "build_ai_preferences_payload",
    "resolve_ai_preferences",
    "validate_ai_preferences",
]

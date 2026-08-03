from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.db import models
from django.utils import timezone

from dateutil import parser

from posthog.models.scoping.root_mixin import TeamScopedRootMixin
from posthog.models.utils import UUIDModel, sane_repr

if TYPE_CHECKING:
    from products.warehouse_sources.backend.models.external_data_schema import ExternalDataSchema

# How a recorded occurrence was explained once its neighbours were taken into account.
CLASSIFICATION_SUSPECTED = "suspected"
CLASSIFICATION_INFRA_BURST = "infra_burst"
CLASSIFICATION_CO_TENANT_VICTIM = "co_tenant_victim"


def infra_burst_window_seconds() -> int:
    return int(getattr(settings, "DATA_WAREHOUSE_OOM_INFRA_BURST_WINDOW_SECONDS", 1800))


def infra_burst_min_schemas() -> int:
    return int(getattr(settings, "DATA_WAREHOUSE_OOM_INFRA_BURST_MIN_SCHEMAS", 50))


def co_tenant_window_seconds() -> int:
    return int(getattr(settings, "DATA_WAREHOUSE_OOM_CO_TENANT_WINDOW_SECONDS", 300))


class ExternalDataSchemaSuspectedOOMEvent(TeamScopedRootMixin, UUIDModel):
    """Append-only log of *suspected* sync OOMs for an external data schema.

    Suspected, not confirmed, and the distinction is the point. What is actually detected is that the
    previous attempt stopped heartbeating, which is equally what a deploy, a pod eviction, a node drain,
    a native crash and a heartbeat lost by a healthy worker look like. Nothing in the signal itself
    distinguishes those from a real out-of-memory kill.

    A row is written once per Temporal retry attempt that follows such a death, so this is an occurrence
    log rather than a counter. `classify` is what narrows it: an occurrence only counts toward the
    repartition trigger when the rows around it cannot explain it as something else.
    """

    # db_constraint=False on the Team FK: a real constraint takes a SHARE ROW EXCLUSIVE lock on the
    # hot posthog_team table on create. Team scoping is enforced at the app layer by TeamScopedRootMixin.
    team = models.ForeignKey("posthog.Team", on_delete=models.CASCADE, db_constraint=False)
    schema = models.ForeignKey(
        "warehouse_sources.ExternalDataSchema", on_delete=models.CASCADE, related_name="suspected_oom_events"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    # Context captured from the prior attempt's last heartbeat.
    run_id = models.CharField(max_length=400, null=True, blank=True)
    host = models.CharField(max_length=400, null=True, blank=True)
    gap_seconds = models.FloatField(null=True, blank=True)
    # The schema's largest measured partition when the row was written. Snapshotted rather than read
    # back live because blame between co-tenants is judged on how big each table was at the time of the
    # kill, and a table that has since been repartitioned would otherwise be judged on its new layout.
    max_partition_bytes = models.BigIntegerField(null=True, blank=True)

    all_teams = models.Manager()  # noqa: DJ012 — both are managers, ruff misclassifies this

    __repr__ = sane_repr("schema_id", "created_at")

    class Meta:
        # Django framework internals (cascade delete, related-object access, prefetch) read through
        # `_default_manager` / `_base_manager` and expect an unfiltered manager. Point them at the plain
        # `all_teams` so a schema delete that cascades to the log doesn't hit the fail-closed manager.
        # `objects` (from TeamScopedRootMixin) stays fail-closed for explicit app code.
        default_manager_name = "all_teams"
        indexes = [
            models.Index(fields=["schema", "created_at"], name="dwh_oom_schema_created_idx"),
            # The two classification lookups below span teams, because both the fleet and a worker pod
            # are shared, so neither can ride the schema-scoped index above.
            models.Index(fields=["created_at"], name="dwh_oom_created_idx"),
            models.Index(fields=["host", "created_at"], name="dwh_oom_host_created_idx"),
        ]

    @classmethod
    def recent_count(cls, schema: "ExternalDataSchema", *, days: int) -> int:
        """Occurrences within the last `days` that nothing else explains, per `classify`.

        `days` is required (no default) so it stays sourced from `DATA_WAREHOUSE_REPARTITION_OOM_WINDOW_DAYS`
        at the call site rather than duplicating that window here where the two could silently diverge.

        The window is also floored at the schema's `last_repartition_at`: a completed repartition addresses
        the OOMs that preceded it, so counting them again would re-trigger a repartition on the same (now
        healthy) table every cooldown until they age out. Only OOMs a repartition did not fix count.
        """
        since = timezone.now() - timedelta(days=days)
        last_repartition_at = schema.last_repartition_at
        if last_repartition_at:
            try:
                since = max(since, parser.parse(last_repartition_at))
            except (ValueError, TypeError):
                pass

        events = list(
            cls.objects.for_team(schema.team_id)
            .filter(schema_id=schema.pk, created_at__gte=since)
            .values("schema_id", "created_at", "host", "max_partition_bytes")
        )
        # Healthy schemas have no rows, so they pay for one indexed lookup and no classification at all.
        return sum(1 for event in events if cls.classify(event) == CLASSIFICATION_SUSPECTED)

    @classmethod
    def classify(cls, event: dict[str, Any]) -> str:
        """Explain one occurrence using the occurrences recorded around it.

        Two things about this failure mode are visible in the log itself, without measuring anything new
        on the worker:

        * Infrastructure takes down many unrelated schemas at once. A deploy, a node drain or a cluster
          incident produces a burst spanning far more schemas than could plausibly have run out of memory
          independently in the same half hour, so a burst is attributed to infrastructure.
        * A pod OOM takes down its co-tenants. A worker runs many activities in one container, so a
          single oversized table kills every schema sharing that pod, and each records an occurrence.
          Within one host's cluster only the largest measured partition is a plausible culprit.

        Returns one of the CLASSIFICATION_* constants. Only `suspected` counts toward repartitioning.
        """
        if cls._is_infra_burst(event["created_at"]):
            return CLASSIFICATION_INFRA_BURST
        if cls._has_larger_co_tenant(event):
            return CLASSIFICATION_CO_TENANT_VICTIM
        return CLASSIFICATION_SUSPECTED

    @classmethod
    def _is_infra_burst(cls, created_at: datetime) -> bool:
        window = timedelta(seconds=infra_burst_window_seconds())
        # COUNT(DISTINCT schema_id) over the created_at index: one number, no rows transferred, however
        # large the burst is.
        distinct_schemas = (
            cls.all_teams.filter(created_at__gte=created_at - window, created_at__lte=created_at + window)
            .values("schema_id")
            .distinct()
            .count()
        )
        return distinct_schemas >= infra_burst_min_schemas()

    @classmethod
    def _has_larger_co_tenant(cls, event: dict[str, Any]) -> bool:
        """Whether a bigger table died on the same worker at the same time.

        Unknown sizes never exonerate: an occurrence with no measurement of its own is left `suspected`
        rather than blamed on a neighbour, so a missing snapshot cannot silently drop a real OOM.
        """
        own_bytes = event["max_partition_bytes"]
        if not event["host"] or own_bytes is None:
            return False
        window = timedelta(seconds=co_tenant_window_seconds())
        largest_co_tenant = (
            cls.all_teams.filter(
                host=event["host"],
                created_at__gte=event["created_at"] - window,
                created_at__lte=event["created_at"] + window,
            )
            .exclude(schema_id=event["schema_id"])
            .aggregate(largest=models.Max("max_partition_bytes"))["largest"]
        )
        return largest_co_tenant is not None and largest_co_tenant > own_bytes

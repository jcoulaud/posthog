"""
Business logic for repository detections.
"""

from django.db import transaction

from products.wizard.backend.facade.contracts import RepositoryDetectionDTO, UpsertRepositoryDetectionInput
from products.wizard.backend.models import RepositoryDetection


def upsert_detection(params: UpsertRepositoryDetectionInput) -> tuple[RepositoryDetectionDTO, bool]:
    """Upsert a detection row and return (dto, created).

    Each push fully replaces `report` / `error` / `task_run_id` — the row always
    reflects the latest detection run for the (repository, kind) key. Concurrent
    POSTs for a brand-new key can race the unique constraint and surface as a
    500; the client's normal HTTP retry handles that on the next attempt.
    """
    with transaction.atomic():
        defaults = {
            "report": params.report,
            "error": params.error,
            "task_run_id": params.task_run_id,
        }
        # created_by only in create_defaults so a later push for the same key can't reattribute it.
        instance, created = RepositoryDetection.objects.update_or_create(
            team_id=params.team_id,
            repository=params.repository,
            kind=params.kind,
            defaults=defaults,
            create_defaults={**defaults, "created_by_id": params.created_by_id},
        )
    return _to_dto(instance), created


def get_detection(team_id: int, repository: str, kind: str) -> RepositoryDetectionDTO | None:
    instance = RepositoryDetection.objects.filter(team_id=team_id, repository=repository, kind=kind).first()
    return _to_dto(instance) if instance else None


def list_detections(
    team_id: int,
    repository: str | None = None,
    kind: str | None = None,
    *,
    offset: int = 0,
    limit: int | None = None,
) -> list[RepositoryDetectionDTO]:
    """List detections for a team, ordered by `updated_at` desc.

    `offset`/`limit` are applied at the SQL layer (LIMIT/OFFSET) so the read
    cost stays bounded regardless of how many detections the team has. The view
    layer should always pass a `limit`.
    """
    qs = RepositoryDetection.objects.filter(team_id=team_id)
    if repository:
        qs = qs.filter(repository=repository)
    if kind:
        qs = qs.filter(kind=kind)
    qs = qs.order_by("-updated_at")
    if limit is not None:
        qs = qs[offset : offset + limit]
    elif offset:
        qs = qs[offset:]
    return [_to_dto(instance) for instance in qs]


def _to_dto(instance: RepositoryDetection) -> RepositoryDetectionDTO:
    return RepositoryDetectionDTO(
        id=str(instance.id),
        team_id=instance.team_id,
        repository=instance.repository,
        kind=instance.kind,
        report=instance.report,
        error=instance.error,
        task_run_id=str(instance.task_run_id) if instance.task_run_id else None,
        created_at=instance.created_at,
        updated_at=instance.updated_at,
    )

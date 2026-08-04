import datetime
import dataclasses
from typing import Any, Optional

from products.warehouse_sources.backend.temporal.data_imports.sources.common.http import make_tracked_session
from products.warehouse_sources.backend.temporal.data_imports.sources.common.rest_source import (
    RESTAPIConfig,
    rest_api_resource,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.rest_source.paginators import (
    JSONResponseCursorPaginator,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.rest_source.typing import EndpointResource
from products.warehouse_sources.backend.temporal.data_imports.sources.common.resumable import ResumableSourceManager
from products.warehouse_sources.backend.temporal.data_imports.sources.common.typings import SourceResponse
from products.warehouse_sources.backend.temporal.data_imports.sources.openalex.settings import (
    OPENALEX_ENDPOINTS,
    OpenAlexEndpointConfig,
)

OPENALEX_BASE_URL = "https://api.openalex.org"
# Maximum the API accepts; anything higher is rejected with "per-page parameter must be
# between 1 and 200".
PAGE_SIZE = 200
REQUEST_TIMEOUT_SECONDS = 60

# Rows are large enough (works carry inverted abstracts and full reference lists) that the
# pipeline default of 5000 rows per chunk can materialize an oversized Arrow table.
LARGE_ROW_CHUNK_SIZE = 1000
LARGE_ROW_CHUNK_SIZE_BYTES = 100 * 1024 * 1024


@dataclasses.dataclass
class OpenAlexResumeConfig:
    cursor: str


def _format_date(value: Any) -> Optional[str]:
    """Render an incremental watermark as the `YYYY-MM-DD` OpenAlex date filters expect."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    return text[:10]


def build_filter(base_filter: Optional[str], from_publication_date: Any = None) -> Optional[str]:
    """Combine the user's scoping filter with the incremental date bound.

    OpenAlex ANDs comma-separated clauses inside a single `filter` param, so both live in one
    string. `from_publication_date` is inclusive, which means each run re-reads the watermark
    day and the merge dedupes it — no gap at the boundary.
    """
    clauses = []
    if base_filter and base_filter.strip():
        clauses.append(base_filter.strip())
    formatted_date = _format_date(from_publication_date)
    if formatted_date:
        clauses.append(f"from_publication_date:{formatted_date}")
    return ",".join(clauses) or None


def get_resource(
    endpoint_config: OpenAlexEndpointConfig,
    entity_filter: Optional[str],
    should_use_incremental_field: bool,
) -> EndpointResource:
    params: dict[str, Any] = {
        "per_page": PAGE_SIZE,
        # Seeds cursor pagination; the paginator replaces it with `meta.next_cursor` from
        # each response.
        "cursor": "*",
        "sort": endpoint_config.sort,
    }

    if should_use_incremental_field:
        params["filter"] = {
            "type": "incremental",
            "cursor_path": "publication_date",
            "initial_value": None,
            "convert": lambda last_value: build_filter(entity_filter, last_value),
        }
    else:
        params["filter"] = build_filter(entity_filter)

    return {
        "name": endpoint_config.name,
        "table_name": endpoint_config.name,
        "write_disposition": {"disposition": "merge", "strategy": "upsert"}
        if should_use_incremental_field
        else "replace",
        "endpoint": {
            "path": endpoint_config.path,
            "data_selector": "results",
            # Every list response wraps its rows in `results`; a body without it is a shape
            # change, not an empty page.
            "data_selector_required": True,
            "params": params,
        },
        "table_format": "delta",
    }


def openalex_source(
    api_key: str,
    endpoint: str,
    entity_filter: Optional[str],
    team_id: int,
    job_id: str,
    resumable_source_manager: ResumableSourceManager[OpenAlexResumeConfig],
    db_incremental_field_last_value: Optional[Any],
    should_use_incremental_field: bool = False,
) -> SourceResponse:
    endpoint_config = OPENALEX_ENDPOINTS[endpoint]

    config: RESTAPIConfig = {
        "client": {
            "base_url": OPENALEX_BASE_URL,
            "auth": {
                "type": "api_key",
                "name": "api_key",
                "api_key": api_key,
                "location": "query",
            },
            "paginator": JSONResponseCursorPaginator(cursor_path="meta.next_cursor", cursor_param="cursor"),
        },
        "resources": [get_resource(endpoint_config, entity_filter, should_use_incremental_field)],
    }

    initial_paginator_state: Optional[dict[str, Any]] = None
    if resumable_source_manager.can_resume():
        resume_config = resumable_source_manager.load_state()
        if resume_config is not None:
            initial_paginator_state = {"cursor": resume_config.cursor}

    def save_checkpoint(state: Optional[dict[str, Any]]) -> None:
        if state and state.get("cursor"):
            resumable_source_manager.save_state(OpenAlexResumeConfig(cursor=str(state["cursor"])))

    resource = rest_api_resource(
        config,
        team_id,
        job_id,
        db_incremental_field_last_value if should_use_incremental_field else None,
        resume_hook=save_checkpoint,
        initial_paginator_state=initial_paginator_state,
    )

    return SourceResponse(
        name=endpoint_config.name,
        items=lambda: resource,
        primary_keys=[endpoint_config.primary_key],
        column_hints=resource.column_hints,
        # Only `works` can be sorted on the incremental field, so it is the only endpoint whose
        # arrival order we can assert. The rest arrive in the API's undocumented cursor order.
        sort_mode="asc" if endpoint_config.sort else None,
        partition_count=1 if endpoint_config.partition_key else None,
        partition_size=1 if endpoint_config.partition_key else None,
        partition_mode="datetime" if endpoint_config.partition_key else None,
        partition_format="month" if endpoint_config.partition_key else None,
        partition_keys=[endpoint_config.partition_key] if endpoint_config.partition_key else None,
        chunk_size=LARGE_ROW_CHUNK_SIZE if endpoint_config.large_rows else None,
        chunk_size_bytes=LARGE_ROW_CHUNK_SIZE_BYTES if endpoint_config.large_rows else None,
    )


def validate_credentials(api_key: str, filters: dict[str, Optional[str]]) -> tuple[bool, Optional[str]]:
    """Probe the API key, then each configured filter expression.

    `/domains` is the cheapest list endpoint (four rows). Filters are checked against their own
    entity because OpenAlex rejects an unknown filter field with a 400 and names it, which is a
    far better setup-time error than a sync that fails on its first page.
    """
    session = make_tracked_session(redact_values=(api_key,))

    probe_params: dict[str, str] = {"per_page": "1", "api_key": api_key}

    response = session.get(
        f"{OPENALEX_BASE_URL}/domains",
        params=probe_params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code in (401, 403):
        return False, "Invalid OpenAlex API key"
    if not response.ok:
        return False, f"OpenAlex API returned {response.status_code}"

    for endpoint, entity_filter in filters.items():
        if not entity_filter or not entity_filter.strip():
            continue
        endpoint_config = OPENALEX_ENDPOINTS[endpoint]
        filter_response = session.get(
            f"{OPENALEX_BASE_URL}{endpoint_config.path}",
            params={**probe_params, "filter": entity_filter.strip()},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if filter_response.status_code == 400:
            return False, f"The {endpoint} filter is not a valid OpenAlex filter expression"
        if filter_response.status_code == 403:
            return False, f"The {endpoint} filter needs an OpenAlex plan your API key does not have"
        if not filter_response.ok:
            return False, f"OpenAlex API returned {filter_response.status_code} for the {endpoint} filter"

    return True, None

from django.db import migrations, models

from posthog.migration_helpers import SafeAddIndexConcurrently


class Migration(migrations.Migration):
    # Concurrent index builds cannot run inside a transaction. Lives in its own
    # migration per PostHog policy (don't mix CONCURRENTLY operations with regular DDL).
    atomic = False
    dependencies = [("warehouse_sources", "0116_suspected_oom_event")]

    operations = [
        SafeAddIndexConcurrently(
            model_name="externaldataschemasuspectedoomevent",
            index=models.Index(fields=["created_at"], name="dwh_oom_created_idx"),
        ),
        SafeAddIndexConcurrently(
            model_name="externaldataschemasuspectedoomevent",
            index=models.Index(fields=["host", "created_at"], name="dwh_oom_host_created_idx"),
        ),
    ]

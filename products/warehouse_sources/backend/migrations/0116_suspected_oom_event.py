from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("warehouse_sources", "0115_scaffold_four_requested_sources")]

    operations = [
        migrations.RenameModel(
            old_name="ExternalDataSchemaOOMEvent",
            new_name="ExternalDataSchemaSuspectedOOMEvent",
        ),
        migrations.AlterField(
            model_name="externaldataschemasuspectedoomevent",
            name="schema",
            field=models.ForeignKey(
                on_delete=models.CASCADE,
                related_name="suspected_oom_events",
                to="warehouse_sources.externaldataschema",
            ),
        ),
        migrations.AddField(
            model_name="externaldataschemasuspectedoomevent",
            name="max_partition_bytes",
            field=models.BigIntegerField(blank=True, null=True),
        ),
    ]

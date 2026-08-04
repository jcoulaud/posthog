from django.db import migrations


def clear_workspace_ai_preferences(apps, schema_editor):
    """Drop AI preferences from the workspace-wide rows.

    A model is a per-project decision and now lives in `TeamTasksConfig`, since one
    Slack workspace can route to several PostHog projects. Only the AI field is
    cleared — these rows also carry the workspace routing default, which stays.
    """
    SlackSettings = apps.get_model("slack_app", "SlackSettings")
    SlackSettings.objects.filter(slack_user_id__isnull=True).exclude(ai_preferences__isnull=True).update(
        ai_preferences=None
    )


class Migration(migrations.Migration):
    dependencies = [
        ("slack_app", "0011_slacksettings_permission_modes"),
    ]

    operations = [
        migrations.RunPython(clear_workspace_ai_preferences, migrations.RunPython.noop, elidable=True),
    ]

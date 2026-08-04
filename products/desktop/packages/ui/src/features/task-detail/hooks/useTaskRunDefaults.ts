import {
  NO_TASK_RUN_DEFAULTS,
  type TaskRunDefaults,
} from "@posthog/api-client/posthog-client";
import { useAuthenticatedQuery } from "@posthog/ui/hooks/useAuthenticatedQuery";
import { useAuthStateValue } from "../../auth/store";

export interface TaskRunDefaultsResult {
  defaults: TaskRunDefaults;
  /** Whether the answer is final — the fetch finished, failed, or was never applicable. */
  isSettled: boolean;
}

/**
 * The project/user default AI run configuration for the signed-in user, which
 * the composer opens on when nothing has been picked locally.
 *
 * Defaults are a nicety: an unauthenticated or failed fetch resolves to "no
 * default" so the composer falls back to its built-in selection rather than
 * stalling.
 */
export function useTaskRunDefaults(): TaskRunDefaultsResult {
  const projectId = useAuthStateValue((state) => state.currentProjectId);
  const query = useAuthenticatedQuery(
    ["task-run-defaults", projectId],
    async (client) => await client.getTaskRunDefaults(Number(projectId)),
    { enabled: projectId != null, staleTime: 5 * 60 * 1000, retry: false },
  );

  return {
    defaults: query.data ?? NO_TASK_RUN_DEFAULTS,
    isSettled: projectId == null || query.isSuccess || query.isError,
  };
}

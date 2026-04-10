import { Hono } from 'hono';

import type { Env } from '../env';
import { AppError } from '../middleware/errors';
import {
  buildPublicHomepagePayloadFromState,
  buildPublicHomepageState,
  serializePublicHomepageState,
} from '../public/homepage';
import {
  listVisibleActiveIncidents,
  listVisibleMaintenanceWindows,
} from '../public/data';
import { refreshPublicHomepageStateAndArtifactIfNeeded } from '../snapshots';
import { parseSettingsPatch, patchSettings, readSettings } from '../settings';

export const adminSettingsRoutes = new Hono<{ Bindings: Env }>();

function queuePublicHomepageSnapshotRefresh(c: { env: Env; executionCtx: ExecutionContext }) {
  const now = Math.floor(Date.now() / 1000);
  c.executionCtx.waitUntil(
    refreshPublicHomepageStateAndArtifactIfNeeded({
      db: c.env.DB,
      now,
      compute: async () => {
        const refreshNow = Math.floor(Date.now() / 1000);
        const includeHiddenMonitors = false;
        const [state, activeIncidents, maintenanceWindows] = await Promise.all([
          buildPublicHomepageState(c.env.DB, refreshNow),
          listVisibleActiveIncidents(c.env.DB, includeHiddenMonitors),
          listVisibleMaintenanceWindows(c.env.DB, refreshNow, includeHiddenMonitors),
        ]);
        const artifactPayload = buildPublicHomepagePayloadFromState({
          state,
          now: refreshNow,
          activeIncidents,
          maintenanceWindows,
          monitorLimit: 12,
        });

        return {
          stateGeneratedAt: state.generated_at,
          stateBodyJson: serializePublicHomepageState(state),
          artifactPayload,
        };
      },
    }).catch((err) => {
      console.warn('homepage snapshot: refresh failed', err);
    }),
  );
}

adminSettingsRoutes.get('/', async (c) => {
  const settings = await readSettings(c.env.DB);
  return c.json({ settings });
});

adminSettingsRoutes.patch('/', async (c) => {
  const rawBody = await c.req.json().catch(() => {
    throw new AppError(400, 'INVALID_ARGUMENT', 'Invalid JSON body');
  });

  const patch = parseSettingsPatch(rawBody);
  await patchSettings(c.env.DB, patch);

  queuePublicHomepageSnapshotRefresh(c);

  const settings = await readSettings(c.env.DB);
  return c.json({ settings });
});

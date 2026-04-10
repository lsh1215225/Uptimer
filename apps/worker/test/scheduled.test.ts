import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../src/monitor/http', () => ({
  runHttpCheck: vi.fn(),
}));
vi.mock('../src/monitor/tcp', () => ({
  runTcpCheck: vi.fn(),
}));
vi.mock('../src/scheduler/lock', () => ({
  acquireLease: vi.fn(),
}));
vi.mock('../src/settings', () => ({
  readSettings: vi.fn(),
}));
vi.mock('../src/notify/webhook', () => ({
  dispatchWebhookToChannels: vi.fn(),
}));
vi.mock('../src/public/homepage', () => ({
  advancePublicHomepageStateCoverageInPlace: vi.fn(),
  buildPublicHomepagePayloadFromState: vi.fn(),
  buildPublicHomepageState: vi.fn(),
  computePublicHomepageArtifactPayload: vi.fn(),
  parsePublicHomepageState: vi.fn(),
  serializePublicHomepageState: vi.fn(),
}));
vi.mock('../src/snapshots', () => ({
  readHomepageArtifactSnapshotGeneratedAt: vi.fn(),
  readHomepageStateSnapshotJson: vi.fn(),
  refreshPublicHomepageArtifactSnapshotIfNeeded: vi.fn(),
  wasHomepageRecentlyAccessed: vi.fn(),
  writeHomepageStateAndArtifactJson: vi.fn(),
}));

import type { Env } from '../src/env';
import { runHttpCheck } from '../src/monitor/http';
import { runTcpCheck } from '../src/monitor/tcp';
import { dispatchWebhookToChannels } from '../src/notify/webhook';
import {
  advancePublicHomepageStateCoverageInPlace,
  buildPublicHomepagePayloadFromState,
  buildPublicHomepageState,
  computePublicHomepageArtifactPayload,
  parsePublicHomepageState,
  serializePublicHomepageState,
} from '../src/public/homepage';
import { runScheduledTick } from '../src/scheduler/scheduled';
import { acquireLease } from '../src/scheduler/lock';
import {
  readHomepageArtifactSnapshotGeneratedAt,
  readHomepageStateSnapshotJson,
  refreshPublicHomepageArtifactSnapshotIfNeeded,
  wasHomepageRecentlyAccessed,
  writeHomepageStateAndArtifactJson,
} from '../src/snapshots';
import { readSettings } from '../src/settings';
import { createFakeD1Database, type FakeD1QueryHandler } from './helpers/fake-d1';

const EMPTY_PUBLIC_CACHE_JSON = JSON.stringify({
  heartbeat: {
    checked_at: [],
    status_codes: '',
    latency_ms: [],
  },
  uptime_days: {
    day_start_at: [],
    total_sec: [],
    downtime_sec: [],
    unknown_sec: [],
    uptime_sec: [],
  },
});

type CreateEnvOptions = {
  dueRows?: unknown[];
  channels?: unknown[];
  suppressedMonitorIds?: number[];
  startedWindows?: unknown[];
  endedWindows?: unknown[];
  windowMonitorLinks?: unknown[];
  onRun?: (normalizedSql: string, args: unknown[]) => void;
};

function createEnv(options: CreateEnvOptions = {}): Env {
  const {
    dueRows = [],
    channels = [],
    suppressedMonitorIds = [],
    startedWindows = [],
    endedWindows = [],
    windowMonitorLinks = [],
    onRun,
  } = options;

  const normalizedDueRows = dueRows.map((row) =>
    Object.defineProperties(
      {
        show_on_status_page: 1,
        created_at: 1_700_000_000 - 40 * 86_400,
        last_checked_at: 1_700_000_000,
        public_cache_json: EMPTY_PUBLIC_CACHE_JSON,
      },
      Object.getOwnPropertyDescriptors(row as Record<string, unknown>),
    ),
  );

  const handlers: FakeD1QueryHandler[] = [
    {
      match: 'from notification_channels',
      all: () => channels,
    },
    {
      match: 'from monitors m',
      all: () => normalizedDueRows,
    },
    {
      match: 'select distinct mwm.monitor_id',
      all: () => suppressedMonitorIds.map((monitor_id) => ({ monitor_id })),
    },
    {
      match: 'from maintenance_windows',
      all: (_args, normalizedSql) => {
        if (normalizedSql.includes('starts_at >=') && normalizedSql.includes('starts_at <=')) {
          return startedWindows;
        }
        if (normalizedSql.includes('ends_at >=') && normalizedSql.includes('ends_at <=')) {
          return endedWindows;
        }
        return [];
      },
    },
    {
      match: 'from incidents',
      all: () => [],
    },
    {
      match: 'from maintenance_window_monitors',
      all: () => windowMonitorLinks,
    },
    {
      match: 'insert into check_results',
      run: (args, normalizedSql) => {
        onRun?.(normalizedSql, args);
        return { meta: { changes: 1 } };
      },
    },
    {
      match: 'insert into monitor_state',
      run: (args, normalizedSql) => {
        onRun?.(normalizedSql, args);
        return { meta: { changes: 1 } };
      },
    },
    {
      match: 'into outages',
      run: (args, normalizedSql) => {
        onRun?.(normalizedSql, args);
        return { meta: { changes: 1 } };
      },
    },
    {
      match: 'update outages',
      run: (args, normalizedSql) => {
        onRun?.(normalizedSql, args);
        return { meta: { changes: 1 } };
      },
    },
  ];

  return {
    DB: createFakeD1Database(handlers),
  } as unknown as Env;
}

describe('scheduler/scheduled regression', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-02-17T00:00:42.000Z'));

    vi.mocked(acquireLease).mockResolvedValue(true);
    vi.mocked(readSettings).mockResolvedValue({
      site_title: 'Uptimer',
      site_description: '',
      site_locale: 'auto',
      site_timezone: 'UTC',
      retention_check_results_days: 7,
      state_failures_to_down_from_up: 2,
      state_successes_to_up_from_down: 2,
      admin_default_overview_range: '24h',
      admin_default_monitor_range: '24h',
      uptime_rating_level: 3,
    });
    vi.mocked(dispatchWebhookToChannels).mockResolvedValue(undefined);
    vi.mocked(advancePublicHomepageStateCoverageInPlace).mockImplementation(() => {});
    vi.mocked(buildPublicHomepagePayloadFromState).mockReturnValue({
      generated_at: Math.floor(Date.now() / 1000),
    } as never);
    vi.mocked(buildPublicHomepageState).mockResolvedValue({
      generated_at: Math.floor(Date.now() / 1000),
    } as never);
    vi.mocked(computePublicHomepageArtifactPayload).mockResolvedValue({
      generated_at: Math.floor(Date.now() / 1000),
    } as never);
    vi.mocked(serializePublicHomepageState).mockReturnValue('{}');
    vi.mocked(readHomepageArtifactSnapshotGeneratedAt).mockResolvedValue(null);
    vi.mocked(readHomepageStateSnapshotJson).mockResolvedValue(null);
    vi.mocked(refreshPublicHomepageArtifactSnapshotIfNeeded).mockResolvedValue(false);
    vi.mocked(wasHomepageRecentlyAccessed).mockResolvedValue(false);
    vi.mocked(writeHomepageStateAndArtifactJson).mockResolvedValue(undefined);
    vi.mocked(runHttpCheck).mockResolvedValue({
      status: 'up',
      latencyMs: 21,
      httpStatus: 200,
      error: null,
      attempts: 1,
    });
    vi.mocked(runTcpCheck).mockResolvedValue({
      status: 'up',
      latencyMs: 12,
      httpStatus: null,
      error: null,
      attempts: 1,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('returns immediately when scheduler lease is not acquired', async () => {
    vi.mocked(acquireLease).mockResolvedValue(false);

    const env = createEnv();
    const waitUntil = vi.fn();

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

    expect(readSettings).not.toHaveBeenCalled();
    expect(waitUntil).not.toHaveBeenCalled();
  });

  it('returns without background work when no monitors are due', async () => {
    const env = createEnv({ dueRows: [] });
    const waitUntil = vi.fn();
    const expectedNow = Math.floor(Date.now() / 1000);

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

    expect(acquireLease).toHaveBeenCalledWith(env.DB, 'scheduler:tick', expectedNow, 55);
    expect(readSettings).toHaveBeenCalledTimes(1);
    expect(wasHomepageRecentlyAccessed).toHaveBeenCalledWith(env.DB, expectedNow);
    expect(refreshPublicHomepageArtifactSnapshotIfNeeded).toHaveBeenCalledWith({
      db: env.DB,
      now: expectedNow,
      compute: expect.any(Function),
    });
    const refreshArgs = vi.mocked(refreshPublicHomepageArtifactSnapshotIfNeeded).mock.calls[0]?.[0];
    expect(refreshArgs).toBeDefined();
    await refreshArgs?.compute();
    expect(computePublicHomepageArtifactPayload).toHaveBeenCalledWith(env.DB, expectedNow);
    expect(waitUntil).toHaveBeenCalledTimes(1);
  });

  it('switches to homepage state and artifact refresh when homepage traffic was seen recently', async () => {
    vi.mocked(wasHomepageRecentlyAccessed).mockResolvedValue(true);

    const env = createEnv({ dueRows: [] });
    const waitUntil = vi.fn();
    const expectedNow = Math.floor(Date.now() / 1000);

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

    expect(wasHomepageRecentlyAccessed).toHaveBeenCalledWith(env.DB, expectedNow);
    expect(refreshPublicHomepageArtifactSnapshotIfNeeded).not.toHaveBeenCalled();
    expect(buildPublicHomepageState).toHaveBeenCalledWith(env.DB, expectedNow);
    expect(buildPublicHomepagePayloadFromState).toHaveBeenCalledWith({
      state: expect.any(Object),
      now: expectedNow,
      activeIncidents: [],
      maintenanceWindows: { active: [], upcoming: [] },
      monitorLimit: 12,
    });
    expect(writeHomepageStateAndArtifactJson).toHaveBeenCalledWith({
      db: env.DB,
      now: expectedNow,
      stateGeneratedAt: expectedNow,
      stateBodyJson: expect.any(String),
      artifactPayload: expect.objectContaining({ generated_at: expectedNow }),
    });
    expect(waitUntil).toHaveBeenCalledTimes(1);
  });

  it('skips homepage refresh after taking the refresh lease when state and artifact are already fresh', async () => {
    vi.mocked(wasHomepageRecentlyAccessed).mockResolvedValue(true);

    const env = createEnv({ dueRows: [] });
    const waitUntil = vi.fn();
    const expectedNow = Math.floor(Date.now() / 1000);

    vi.mocked(readHomepageStateSnapshotJson)
      .mockResolvedValueOnce({
        generatedAt: expectedNow - 60,
        bodyJson: '{}',
      })
      .mockResolvedValueOnce({
        generatedAt: expectedNow,
        bodyJson: '{}',
      });
    vi.mocked(readHomepageArtifactSnapshotGeneratedAt)
      .mockResolvedValueOnce(expectedNow - 60)
      .mockResolvedValueOnce(expectedNow);

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

    expect(buildPublicHomepageState).not.toHaveBeenCalled();
    expect(buildPublicHomepagePayloadFromState).not.toHaveBeenCalled();
    expect(writeHomepageStateAndArtifactJson).not.toHaveBeenCalled();
  });

  it('refreshes homepage state and artifact directly from the stored homepage state snapshot', async () => {
    vi.mocked(wasHomepageRecentlyAccessed).mockResolvedValue(true);

    const env = createEnv({
      dueRows: [
        {
          id: 101,
          name: 'API',
          type: 'http',
          target: 'https://example.com/health',
          interval_sec: 60,
          timeout_ms: 5000,
          http_method: 'GET',
          http_headers_json: null,
          http_body: null,
          expected_status_json: null,
          response_keyword: null,
          response_keyword_mode: null,
          response_forbidden_keyword: null,
          response_forbidden_keyword_mode: null,
          state_status: 'up',
          state_last_error: null,
          last_changed_at: 1700000000,
          consecutive_failures: 0,
          consecutive_successes: 3,
        },
      ],
    });
    const waitUntil = vi.fn();
    const expectedNow = Math.floor(Date.now() / 1000);
    const storedState = {
      generated_at: expectedNow - 60,
      monitor_count_total: 1,
      site_title: 'Status Hub',
      site_description: 'Production services',
      site_locale: 'en',
      site_timezone: 'UTC',
      uptime_rating_level: 4,
      monitors: [
        {
          id: 101,
          name: 'API',
          type: 'http',
          group_name: 'Core',
          interval_sec: 60,
          created_at: expectedNow - 40 * 86400,
          state_status: 'up',
          last_checked_at: expectedNow - 60,
          covered_until_at: expectedNow - 60,
          cache: {
            heartbeat: {
              checked_at: [],
              status_codes: '',
              latency_ms: [],
            },
            uptime_days: {
              day_start_at: [],
              total_sec: [],
              downtime_sec: [],
              unknown_sec: [],
              uptime_sec: [],
            },
          },
        },
      ],
      resolved_incident_preview: null,
      maintenance_history_preview: null,
    };

    vi.mocked(readHomepageStateSnapshotJson)
      .mockResolvedValueOnce({
        generatedAt: expectedNow - 60,
        bodyJson: '{"stale":true}',
      })
      .mockResolvedValueOnce({
        generatedAt: expectedNow - 60,
        bodyJson: '{"stale":true}',
      });
    vi.mocked(readHomepageArtifactSnapshotGeneratedAt)
      .mockResolvedValueOnce(expectedNow - 60)
      .mockResolvedValueOnce(expectedNow - 60);
    vi.mocked(parsePublicHomepageState).mockReturnValue(storedState as never);
    vi.mocked(advancePublicHomepageStateCoverageInPlace).mockImplementation((state, now) => {
      (state as { generated_at: number }).generated_at = now;
    });

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

    expect(parsePublicHomepageState).toHaveBeenCalledTimes(1);
    expect(buildPublicHomepageState).not.toHaveBeenCalled();
    expect(buildPublicHomepagePayloadFromState).toHaveBeenCalledWith({
      state: expect.any(Object),
      now: expectedNow,
      activeIncidents: [],
      maintenanceWindows: { active: [], upcoming: [] },
      monitorLimit: 12,
    });
    expect(writeHomepageStateAndArtifactJson).toHaveBeenCalledWith({
      db: env.DB,
      now: expectedNow,
      stateGeneratedAt: expectedNow,
      stateBodyJson: '{}',
      artifactPayload: expect.objectContaining({ generated_at: expectedNow }),
    });
  });

  it('logs homepage snapshot refresh failures without breaking the tick', async () => {
    vi.mocked(refreshPublicHomepageArtifactSnapshotIfNeeded).mockRejectedValueOnce(
      new Error('snapshot refresh failed'),
    );

    const env = createEnv({ dueRows: [] });
    const waitUntil = vi.fn();
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

    try {
      await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

      expect(waitUntil).toHaveBeenCalledTimes(1);
      await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

      expect(warnSpy).toHaveBeenCalledWith(
        'homepage snapshot: refresh failed',
        expect.any(Error),
      );
    } finally {
      warnSpy.mockRestore();
    }
  });

  it('processes due HTTP monitors and writes check/state rows', async () => {
    const runSql: string[] = [];
    const runArgs: unknown[][] = [];
    const dueRows = [
      {
        id: 101,
        name: 'API',
        type: 'http',
        target: 'https://example.com/health',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 0,
        consecutive_successes: 3,
      },
    ];
    const env = createEnv({
      dueRows,
      onRun: (sql, args) => {
        runSql.push(sql);
        runArgs.push(args);
      },
    });
    const waitUntil = vi.fn();
    const expectedCheckedAt = Math.floor(Math.floor(Date.now() / 1000) / 60) * 60;

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

    expect(runHttpCheck).toHaveBeenCalledWith({
      url: 'https://example.com/health',
      timeoutMs: 5000,
      method: 'GET',
      headers: null,
      body: null,
      expectedStatus: null,
      responseKeyword: null,
      responseKeywordMode: null,
      responseForbiddenKeyword: null,
      responseForbiddenKeywordMode: null,
    });

    const checkInsertIndex = runSql.findIndex((sql) => sql.includes('insert into check_results'));
    expect(checkInsertIndex).toBeGreaterThan(-1);
    expect(runArgs[checkInsertIndex]).toEqual([
      101,
      expectedCheckedAt,
      'up',
      21,
      200,
      null,
      null,
      1,
    ]);

    const stateUpsertIndex = runSql.findIndex((sql) => sql.includes('insert into monitor_state'));
    expect(stateUpsertIndex).toBeGreaterThan(-1);
    expect(runArgs[stateUpsertIndex]?.[0]).toBe(101);
    expect(runArgs[stateUpsertIndex]?.[1]).toBe('up');
    expect(runArgs[stateUpsertIndex]?.[2]).toBe(expectedCheckedAt);

    expect(refreshPublicHomepageArtifactSnapshotIfNeeded).toHaveBeenCalledTimes(1);
    expect(waitUntil).toHaveBeenCalledTimes(1);
  });

  it('passes explicit response assertion modes through scheduled HTTP checks', async () => {
    const dueRows = [
      {
        id: 102,
        name: 'Regex API',
        type: 'http',
        target: 'https://example.com/regex',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: '^ready:\\\\d+$',
        response_keyword_mode: 'regex',
        response_forbidden_keyword: 'error',
        response_forbidden_keyword_mode: 'contains',
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 0,
        consecutive_successes: 2,
      },
    ];
    const env = createEnv({ dueRows });

    await runScheduledTick(env, { waitUntil: vi.fn() } as unknown as ExecutionContext);

    expect(runHttpCheck).toHaveBeenCalledWith({
      url: 'https://example.com/regex',
      timeoutMs: 5000,
      method: 'GET',
      headers: null,
      body: null,
      expectedStatus: null,
      responseKeyword: '^ready:\\\\d+$',
      responseKeywordMode: 'regex',
      responseForbiddenKeyword: 'error',
      responseForbiddenKeywordMode: 'contains',
    });
  });

  it('batches persistence for multiple due monitors', async () => {
    const dueRows = [
      {
        id: 111,
        name: 'API A',
        type: 'http',
        target: 'https://example.com/a',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 0,
        consecutive_successes: 3,
      },
      {
        id: 112,
        name: 'API B',
        type: 'http',
        target: 'https://example.com/b',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 0,
        consecutive_successes: 2,
      },
    ];
    const env = createEnv({ dueRows });
    const batchSpy = vi.spyOn(env.DB, 'batch');

    await runScheduledTick(env, { waitUntil: vi.fn() } as unknown as ExecutionContext);

    expect(runHttpCheck).toHaveBeenCalledTimes(2);
    expect(batchSpy).toHaveBeenCalledTimes(1);
  });

  it('sends monitor.down notification when status changes and monitor is not suppressed', async () => {
    vi.mocked(runHttpCheck).mockResolvedValue({
      status: 'down',
      latencyMs: 123,
      httpStatus: 503,
      error: 'HTTP 503',
      attempts: 2,
    });

    const dueRows = [
      {
        id: 201,
        name: 'Core API',
        type: 'http',
        target: 'https://api.example.com/health',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 1,
        consecutive_successes: 0,
      },
    ];
    const channels = [
      {
        id: 1,
        name: 'primary',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: 1700000000,
      },
    ];
    const env = createEnv({ dueRows, channels });
    const waitUntil = vi.fn();
    const expectedCheckedAt = Math.floor(Math.floor(Date.now() / 1000) / 60) * 60;

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

    expect(waitUntil).toHaveBeenCalledTimes(2);
    await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

    expect(dispatchWebhookToChannels).toHaveBeenCalledWith(
      expect.objectContaining({
        db: env.DB,
        eventType: 'monitor.down',
        eventKey: `monitor:201:down:${expectedCheckedAt}`,
        payload: expect.objectContaining({
          event: 'monitor.down',
          monitor: expect.objectContaining({
            id: 201,
            name: 'Core API',
          }),
          state: expect.objectContaining({
            status: 'down',
            http_status: 503,
            error: 'HTTP 503',
          }),
        }),
      }),
    );
  });

  it('suppresses monitor notifications during active maintenance windows', async () => {
    vi.mocked(runHttpCheck).mockResolvedValue({
      status: 'down',
      latencyMs: 91,
      httpStatus: 503,
      error: 'HTTP 503',
      attempts: 1,
    });
    const dueRows = [
      {
        id: 301,
        name: 'Billing API',
        type: 'http',
        target: 'https://billing.example.com/health',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'unknown',
        state_last_error: null,
        last_changed_at: null,
        consecutive_failures: 0,
        consecutive_successes: 0,
      },
    ];
    const channels = [
      {
        id: 7,
        name: 'primary',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: 1700000000,
      },
    ];
    const env = createEnv({
      dueRows,
      channels,
      suppressedMonitorIds: [301],
    });

    await runScheduledTick(env, { waitUntil: vi.fn() } as unknown as ExecutionContext);
    expect(dispatchWebhookToChannels).not.toHaveBeenCalledWith(
      expect.objectContaining({ eventType: 'monitor.down' }),
    );
  });

  it('sends monitor.up when a down monitor recovers', async () => {
    vi.mocked(runHttpCheck).mockResolvedValue({
      status: 'up',
      latencyMs: 45,
      httpStatus: 200,
      error: null,
      attempts: 1,
    });

    const dueRows = [
      {
        id: 302,
        name: 'Recovery API',
        type: 'http',
        target: 'https://recovery.example.com/health',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'down',
        state_last_error: 'HTTP 503',
        last_changed_at: 1700000000,
        consecutive_failures: 2,
        consecutive_successes: 1,
      },
    ];
    const channels = [
      {
        id: 8,
        name: 'primary',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: 1700000000,
      },
    ];
    const env = createEnv({ dueRows, channels });
    const waitUntil = vi.fn();
    const expectedCheckedAt = Math.floor(Math.floor(Date.now() / 1000) / 60) * 60;

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);
    expect(waitUntil).toHaveBeenCalledTimes(2);
    await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

    expect(dispatchWebhookToChannels).toHaveBeenCalledWith(
      expect.objectContaining({
        eventType: 'monitor.up',
        eventKey: `monitor:302:up:${expectedCheckedAt}`,
      }),
    );
  });

  it('runs tcp checks for tcp monitor rows', async () => {
    vi.mocked(runTcpCheck).mockResolvedValue({
      status: 'down',
      latencyMs: 70,
      httpStatus: null,
      error: 'connection refused',
      attempts: 2,
    });

    const runSql: string[] = [];
    const runArgs: unknown[][] = [];
    const dueRows = [
      {
        id: 401,
        name: 'TCP Service',
        type: 'tcp',
        target: 'example.com:5432',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: null,
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 1,
        consecutive_successes: 0,
      },
    ];
    const env = createEnv({
      dueRows,
      onRun: (sql, args) => {
        runSql.push(sql);
        runArgs.push(args);
      },
    });

    await runScheduledTick(env, { waitUntil: vi.fn() } as unknown as ExecutionContext);
    expect(runTcpCheck).toHaveBeenCalledWith({
      target: 'example.com:5432',
      timeoutMs: 5000,
    });

    const checkInsertIndex = runSql.findIndex((sql) => sql.includes('insert into check_results'));
    expect(checkInsertIndex).toBeGreaterThan(-1);
    expect(runArgs[checkInsertIndex]?.[2]).toBe('down');
    expect(runArgs[checkInsertIndex]?.[7]).toBe(2);
  });

  it('logs failed due monitor runs and still schedules homepage refresh', async () => {
    const dueRows = [
      {
        id: 501,
        name: 'Broken API',
        type: 'http',
        target: 'https://broken.example.com/health',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        last_changed_at: 1700000000,
        consecutive_failures: 0,
        consecutive_successes: 2,
      },
    ];
    Object.defineProperty(dueRows[0] as Record<string, unknown>, 'state_last_error', {
      get() {
        throw new Error('corrupt state row');
      },
    });
    const env = createEnv({ dueRows });
    const waitUntil = vi.fn();
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

      expect(errorSpy).toHaveBeenCalledWith(
        'scheduled: 1/1 monitors failed',
        expect.objectContaining({
          status: 'rejected',
          reason: expect.any(Error),
        }),
      );
      expect(waitUntil).toHaveBeenCalledTimes(1);
    } finally {
      errorSpy.mockRestore();
    }
  });

  it('logs failed monitor notification dispatches', async () => {
    vi.mocked(runHttpCheck).mockResolvedValue({
      status: 'down',
      latencyMs: 123,
      httpStatus: 503,
      error: 'HTTP 503',
      attempts: 2,
    });
    vi.mocked(dispatchWebhookToChannels).mockRejectedValueOnce(new Error('webhook unavailable'));

    const dueRows = [
      {
        id: 502,
        name: 'Core API',
        type: 'http',
        target: 'https://api.example.com/health',
        interval_sec: 60,
        timeout_ms: 5000,
        http_method: 'GET',
        http_headers_json: null,
        http_body: null,
        expected_status_json: null,
        response_keyword: null,
        response_keyword_mode: null,
        response_forbidden_keyword: null,
        response_forbidden_keyword_mode: null,
        state_status: 'up',
        state_last_error: null,
        last_changed_at: 1700000000,
        consecutive_failures: 1,
        consecutive_successes: 0,
      },
    ];
    const channels = [
      {
        id: 12,
        name: 'primary',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: 1700000000,
      },
    ];
    const env = createEnv({ dueRows, channels });
    const waitUntil = vi.fn();
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

      expect(waitUntil).toHaveBeenCalledTimes(2);
      await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

      expect(errorSpy).toHaveBeenCalledWith(
        'notify: failed to dispatch webhooks',
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });

  it('emits maintenance started/ended notifications using created_at gating', async () => {
    const now = Math.floor(Date.now() / 1000);
    const startedAt = now - 60;
    const endedAt = now - 20;
    const channels = [
      {
        id: 10,
        name: 'older',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: startedAt - 10,
      },
      {
        id: 11,
        name: 'newer',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: endedAt + 10,
      },
    ];
    const env = createEnv({
      dueRows: [],
      channels,
      startedWindows: [
        {
          id: 1,
          title: 'Deploy',
          message: null,
          starts_at: startedAt,
          ends_at: endedAt,
          created_at: startedAt - 100,
        },
      ],
      endedWindows: [
        {
          id: 1,
          title: 'Deploy',
          message: null,
          starts_at: startedAt,
          ends_at: endedAt,
          created_at: startedAt - 100,
        },
      ],
      windowMonitorLinks: [{ maintenance_window_id: 1, monitor_id: 301 }],
    });
    const waitUntil = vi.fn();

    await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

    expect(waitUntil).toHaveBeenCalledTimes(3);
    await Promise.all(waitUntil.mock.calls.map((c) => c[0] as Promise<unknown>));

    expect(dispatchWebhookToChannels).toHaveBeenCalledWith(
      expect.objectContaining({
        eventType: 'maintenance.started',
        eventKey: `maintenance:1:started:${startedAt}`,
        channels: [
          expect.objectContaining({
            id: 10,
          }),
        ],
      }),
    );
    expect(dispatchWebhookToChannels).toHaveBeenCalledWith(
      expect.objectContaining({
        eventType: 'maintenance.ended',
        eventKey: `maintenance:1:ended:${endedAt}`,
        // channel 11 was created after endedAt and should be excluded.
        channels: [expect.objectContaining({ id: 10 })],
      }),
    );
  });

  it('logs failed maintenance notification dispatches', async () => {
    const now = Math.floor(Date.now() / 1000);
    const startedAt = now - 60;
    const endedAt = now - 20;
    const channels = [
      {
        id: 13,
        name: 'older',
        config_json: JSON.stringify({
          url: 'https://hooks.example.com/uptimer',
          method: 'POST',
          payload_type: 'json',
        }),
        created_at: startedAt - 10,
      },
    ];
    vi.mocked(dispatchWebhookToChannels).mockRejectedValue(new Error('maintenance webhook failed'));

    const env = createEnv({
      dueRows: [],
      channels,
      startedWindows: [
        {
          id: 2,
          title: 'Deploy',
          message: null,
          starts_at: startedAt,
          ends_at: endedAt,
          created_at: startedAt - 100,
        },
      ],
      endedWindows: [
        {
          id: 2,
          title: 'Deploy',
          message: null,
          starts_at: startedAt,
          ends_at: endedAt,
          created_at: startedAt - 100,
        },
      ],
      windowMonitorLinks: [{ maintenance_window_id: 2, monitor_id: 301 }],
    });
    const waitUntil = vi.fn();
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await runScheduledTick(env, { waitUntil } as unknown as ExecutionContext);

      expect(waitUntil).toHaveBeenCalledTimes(3);
      await Promise.all(waitUntil.mock.calls.map((call) => call[0] as Promise<unknown>));

      expect(errorSpy).toHaveBeenCalledWith(
        'notify: failed to dispatch maintenance.started',
        expect.any(Error),
      );
      expect(errorSpy).toHaveBeenCalledWith(
        'notify: failed to dispatch maintenance.ended',
        expect.any(Error),
      );
    } finally {
      errorSpy.mockRestore();
    }
  });
});

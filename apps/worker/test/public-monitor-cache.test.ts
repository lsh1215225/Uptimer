import { describe, expect, it } from 'vitest';

import {
  appendHeartbeatToPublicMonitorCache,
  createEmptyPublicMonitorCache,
  extendPublicMonitorCacheToTime,
  materializePublicMonitorCache,
  parsePublicMonitorCache,
  serializePublicMonitorCache,
} from '../src/public/monitor-cache';

describe('public monitor cache', () => {
  it('extends cached uptime with live uptime and trailing unknown time for overdue monitors', () => {
    const now = 1_728_000_000;
    const expectedDayStart = Math.floor((now - 1) / 86_400) * 86_400;
    const cache = createEmptyPublicMonitorCache();

    const extended = extendPublicMonitorCacheToTime(cache, {
      status: 'up',
      lastCheckedAt: now - 180,
      intervalSec: 60,
      createdAt: now - 40 * 86_400,
      now,
    });
    const materialized = materializePublicMonitorCache(extended);

    expect(materialized.uptime_day_strip.day_start_at).toEqual([expectedDayStart]);
    expect(materialized.uptime_day_strip.unknown_sec).toEqual([60]);
    expect(materialized.uptime_30d?.uptime_pct).toBeCloseTo(66.666, 2);
  });

  it('appends newest heartbeat samples at the front of the cache', () => {
    const now = 1_728_000_000;
    const cache = appendHeartbeatToPublicMonitorCache(createEmptyPublicMonitorCache(), {
      checkedAt: now - 60,
      status: 'down',
      latencyMs: null,
    });
    const next = appendHeartbeatToPublicMonitorCache(cache, {
      checkedAt: now,
      status: 'up',
      latencyMs: 42,
    });

    expect(next.heartbeat.checked_at).toEqual([now, now - 60]);
    expect(next.heartbeat.status_codes).toBe('ud');
    expect(next.heartbeat.latency_ms).toEqual([42, null]);
  });

  it('round-trips serialized cache payloads', () => {
    const cache = appendHeartbeatToPublicMonitorCache(createEmptyPublicMonitorCache(), {
      checkedAt: 100,
      status: 'up',
      latencyMs: 12,
    });

    expect(parsePublicMonitorCache(serializePublicMonitorCache(cache))).toEqual(cache);
  });
});

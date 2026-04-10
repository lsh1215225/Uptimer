import { z } from 'zod';

import { computeTodayPartialUptimeBatch, utcDayStart } from './data';
import { buildNumberedPlaceholders } from './visibility';

const HEARTBEAT_POINTS = 60;
const UPTIME_DAYS = 30;

const publicMonitorHeartbeatCacheSchema = z.object({
  checked_at: z.array(z.number().int()),
  status_codes: z.string(),
  latency_ms: z.array(z.number().int().nullable()),
});

const publicMonitorUptimeCacheSchema = z.object({
  day_start_at: z.array(z.number().int()),
  total_sec: z.array(z.number().int()),
  downtime_sec: z.array(z.number().int()),
  unknown_sec: z.array(z.number().int()),
  uptime_sec: z.array(z.number().int()),
});

const publicMonitorCacheSchema = z.object({
  heartbeat: publicMonitorHeartbeatCacheSchema,
  uptime_days: publicMonitorUptimeCacheSchema,
});

export type PublicMonitorCache = z.infer<typeof publicMonitorCacheSchema>;

export type PublicMonitorCacheSeedRow = {
  id: number;
  interval_sec: number;
  created_at: number;
  last_checked_at: number | null;
};

type HeartbeatQueryRow = {
  monitor_id: number;
  checked_at: number;
  status: string;
  latency_ms: number | null;
};

type UptimeRollupRow = {
  monitor_id: number;
  day_start_at: number;
  total_sec: number;
  downtime_sec: number;
  unknown_sec: number;
  uptime_sec: number;
};

export type MaterializedPublicMonitorCache = {
  heartbeat_strip: {
    checked_at: number[];
    status_codes: string;
    latency_ms: Array<number | null>;
  };
  uptime_day_strip: {
    day_start_at: number[];
    downtime_sec: number[];
    unknown_sec: number[];
    uptime_pct_milli: Array<number | null>;
  };
  uptime_30d: { uptime_pct: number } | null;
};

type SegmentStatus = 'up' | 'down' | 'unknown' | 'maintenance' | 'paused' | null;

function toHeartbeatStatusCode(status: string | null | undefined): string {
  switch (status) {
    case 'up':
      return 'u';
    case 'down':
      return 'd';
    case 'maintenance':
      return 'm';
    case 'unknown':
    default:
      return 'x';
  }
}

function normalizeHeartbeatCache(
  input: PublicMonitorCache['heartbeat'],
): PublicMonitorCache['heartbeat'] {
  const length = Math.min(
    HEARTBEAT_POINTS,
    input.checked_at.length,
    input.latency_ms.length,
    input.status_codes.length,
  );

  return {
    checked_at: input.checked_at.slice(0, length),
    status_codes: input.status_codes.slice(0, length),
    latency_ms: input.latency_ms.slice(0, length),
  };
}

function normalizeUptimeDaysCache(
  input: PublicMonitorCache['uptime_days'],
): PublicMonitorCache['uptime_days'] {
  const length = Math.min(
    input.day_start_at.length,
    input.total_sec.length,
    input.downtime_sec.length,
    input.unknown_sec.length,
    input.uptime_sec.length,
  );
  const boundedLength = Math.min(length, UPTIME_DAYS);
  const start = Math.max(0, length - boundedLength);

  return {
    day_start_at: input.day_start_at.slice(start, length),
    total_sec: input.total_sec.slice(start, length),
    downtime_sec: input.downtime_sec.slice(start, length),
    unknown_sec: input.unknown_sec.slice(start, length),
    uptime_sec: input.uptime_sec.slice(start, length),
  };
}

function normalizePublicMonitorCache(cache: PublicMonitorCache): PublicMonitorCache {
  return {
    heartbeat: normalizeHeartbeatCache(cache.heartbeat),
    uptime_days: normalizeUptimeDaysCache(cache.uptime_days),
  };
}

function clonePublicMonitorCache(cache: PublicMonitorCache): PublicMonitorCache {
  return {
    heartbeat: {
      checked_at: [...cache.heartbeat.checked_at],
      status_codes: cache.heartbeat.status_codes,
      latency_ms: [...cache.heartbeat.latency_ms],
    },
    uptime_days: {
      day_start_at: [...cache.uptime_days.day_start_at],
      total_sec: [...cache.uptime_days.total_sec],
      downtime_sec: [...cache.uptime_days.downtime_sec],
      unknown_sec: [...cache.uptime_days.unknown_sec],
      uptime_sec: [...cache.uptime_days.uptime_sec],
    },
  };
}

function ensureUptimeDayIndex(
  cache: PublicMonitorCache['uptime_days'],
  dayStartAt: number,
): number {
  const existingIndex = cache.day_start_at.indexOf(dayStartAt);
  if (existingIndex >= 0) {
    return existingIndex;
  }

  cache.day_start_at.push(dayStartAt);
  cache.total_sec.push(0);
  cache.downtime_sec.push(0);
  cache.unknown_sec.push(0);
  cache.uptime_sec.push(0);

  return cache.day_start_at.length - 1;
}

function trimUptimeDaysWindow(cache: PublicMonitorCache['uptime_days'], now: number): void {
  const minDayStart = utcDayStart(now) - (UPTIME_DAYS - 1) * 86_400;

  while (cache.day_start_at.length > 0) {
    const dayStartAt = cache.day_start_at[0];
    if (dayStartAt === undefined || dayStartAt >= minDayStart) {
      break;
    }

    cache.day_start_at.shift();
    cache.total_sec.shift();
    cache.downtime_sec.shift();
    cache.unknown_sec.shift();
    cache.uptime_sec.shift();
  }

  while (cache.day_start_at.length > UPTIME_DAYS) {
    cache.day_start_at.shift();
    cache.total_sec.shift();
    cache.downtime_sec.shift();
    cache.unknown_sec.shift();
    cache.uptime_sec.shift();
  }
}

function addBucketSeconds(
  cache: PublicMonitorCache['uptime_days'],
  dayStartAt: number,
  seconds: { total: number; downtime: number; unknown: number; uptime: number },
): void {
  if (seconds.total <= 0) {
    return;
  }

  const index = ensureUptimeDayIndex(cache, dayStartAt);
  cache.total_sec[index] = (cache.total_sec[index] ?? 0) + seconds.total;
  cache.downtime_sec[index] = (cache.downtime_sec[index] ?? 0) + seconds.downtime;
  cache.unknown_sec[index] = (cache.unknown_sec[index] ?? 0) + seconds.unknown;
  cache.uptime_sec[index] = (cache.uptime_sec[index] ?? 0) + seconds.uptime;
}

function applyRealtimeSegment(
  cache: PublicMonitorCache,
  opts: {
    status: SegmentStatus;
    checkedAt: number;
    intervalSec: number;
    createdAt: number;
    from: number | null;
    to: number;
  },
): void {
  if (opts.from === null) {
    return;
  }

  const segmentStart = Math.max(opts.from, opts.createdAt);
  if (opts.to <= segmentStart) {
    return;
  }

  if (
    opts.status !== 'up' &&
    opts.status !== 'down' &&
    opts.status !== 'unknown'
  ) {
    return;
  }

  const validUntil =
    opts.status === 'up' && Number.isFinite(opts.intervalSec) && opts.intervalSec > 0
      ? opts.checkedAt + opts.intervalSec * 2
      : opts.checkedAt;

  let cursor = segmentStart;

  while (cursor < opts.to) {
    const dayStartAt = utcDayStart(cursor);
    const dayEnd = Math.min(opts.to, dayStartAt + 86_400);
    if (dayEnd <= cursor) {
      break;
    }

    if (opts.status === 'down') {
      addBucketSeconds(cache.uptime_days, dayStartAt, {
        total: dayEnd - cursor,
        downtime: dayEnd - cursor,
        unknown: 0,
        uptime: 0,
      });
    } else if (opts.status === 'unknown') {
      addBucketSeconds(cache.uptime_days, dayStartAt, {
        total: dayEnd - cursor,
        downtime: 0,
        unknown: dayEnd - cursor,
        uptime: 0,
      });
    } else {
      const uptimeEnd = Math.min(dayEnd, Math.max(cursor, validUntil));
      const uptimeSeconds = Math.max(0, uptimeEnd - cursor);
      const unknownSeconds = Math.max(0, dayEnd - uptimeEnd);

      addBucketSeconds(cache.uptime_days, dayStartAt, {
        total: dayEnd - cursor,
        downtime: 0,
        unknown: unknownSeconds,
        uptime: uptimeSeconds,
      });
    }

    cursor = dayEnd;
  }
}

function buildCacheFromRows(opts: {
  heartbeats: HeartbeatQueryRow[];
  uptimeDays: Array<{
    day_start_at: number;
    total_sec: number;
    downtime_sec: number;
    unknown_sec: number;
    uptime_sec: number;
  }>;
}): PublicMonitorCache {
  const heartbeatStatusCodes = new Array<string>(opts.heartbeats.length);
  for (let index = 0; index < opts.heartbeats.length; index += 1) {
    heartbeatStatusCodes[index] = toHeartbeatStatusCode(opts.heartbeats[index]?.status);
  }

  return normalizePublicMonitorCache({
    heartbeat: {
      checked_at: opts.heartbeats.map((row) => row.checked_at),
      status_codes: heartbeatStatusCodes.join(''),
      latency_ms: opts.heartbeats.map((row) => row.latency_ms),
    },
    uptime_days: {
      day_start_at: opts.uptimeDays.map((row) => row.day_start_at),
      total_sec: opts.uptimeDays.map((row) => row.total_sec),
      downtime_sec: opts.uptimeDays.map((row) => row.downtime_sec),
      unknown_sec: opts.uptimeDays.map((row) => row.unknown_sec),
      uptime_sec: opts.uptimeDays.map((row) => row.uptime_sec),
    },
  });
}

export function createEmptyPublicMonitorCache(): PublicMonitorCache {
  return {
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
  };
}

export function parsePublicMonitorCache(value: string | null | undefined): PublicMonitorCache | null {
  if (!value) {
    return null;
  }

  try {
    const parsed = JSON.parse(value) as unknown;
    const result = publicMonitorCacheSchema.safeParse(parsed);
    if (!result.success) {
      return null;
    }
    return normalizePublicMonitorCache(result.data);
  } catch {
    return null;
  }
}

export function serializePublicMonitorCache(cache: PublicMonitorCache): string {
  return JSON.stringify(normalizePublicMonitorCache(cache));
}

export function appendHeartbeatToPublicMonitorCache(
  cache: PublicMonitorCache,
  entry: { checkedAt: number; status: string; latencyMs: number | null },
): PublicMonitorCache {
  const next = clonePublicMonitorCache(cache);
  appendHeartbeatToPublicMonitorCacheInPlace(next, entry);
  return next;
}

export function appendHeartbeatToPublicMonitorCacheInPlace(
  cache: PublicMonitorCache,
  entry: { checkedAt: number; status: string; latencyMs: number | null },
): void {
  const nextStatusCode = toHeartbeatStatusCode(entry.status);
  const currentHead = cache.heartbeat.checked_at[0];

  if (currentHead === entry.checkedAt) {
    cache.heartbeat.checked_at[0] = entry.checkedAt;
    cache.heartbeat.latency_ms[0] = entry.latencyMs;
    cache.heartbeat.status_codes =
      nextStatusCode + cache.heartbeat.status_codes.slice(1);
  } else {
    cache.heartbeat.checked_at.unshift(entry.checkedAt);
    cache.heartbeat.latency_ms.unshift(entry.latencyMs);
    cache.heartbeat.status_codes = nextStatusCode + cache.heartbeat.status_codes;
  }

  cache.heartbeat.checked_at = cache.heartbeat.checked_at.slice(0, HEARTBEAT_POINTS);
  cache.heartbeat.latency_ms = cache.heartbeat.latency_ms.slice(0, HEARTBEAT_POINTS);
  cache.heartbeat.status_codes = cache.heartbeat.status_codes.slice(0, HEARTBEAT_POINTS);
}

export function extendPublicMonitorCacheToTime(
  cache: PublicMonitorCache,
  opts: {
    status: SegmentStatus;
    lastCheckedAt: number | null;
    intervalSec: number;
    createdAt: number;
    now: number;
  },
): PublicMonitorCache {
  const next = clonePublicMonitorCache(cache);
  applyRealtimeSegment(next, {
    status: opts.status,
    checkedAt: opts.lastCheckedAt ?? 0,
    intervalSec: opts.intervalSec,
    createdAt: opts.createdAt,
    from: opts.lastCheckedAt,
    to: opts.now,
  });
  trimUptimeDaysWindow(next.uptime_days, opts.now);
  return next;
}

export function advancePublicMonitorCacheCoverage(
  cache: PublicMonitorCache,
  opts: {
    status: SegmentStatus;
    checkedAt: number;
    intervalSec: number;
    createdAt: number;
    from: number | null;
    to: number;
  },
): PublicMonitorCache {
  const next = clonePublicMonitorCache(cache);
  advancePublicMonitorCacheCoverageInPlace(next, opts);
  return next;
}

export function advancePublicMonitorCacheCoverageInPlace(
  cache: PublicMonitorCache,
  opts: {
    status: SegmentStatus;
    checkedAt: number;
    intervalSec: number;
    createdAt: number;
    from: number | null;
    to: number;
  },
): void {
  applyRealtimeSegment(cache, opts);
  trimUptimeDaysWindow(cache.uptime_days, opts.to);
}

export function materializePublicMonitorCache(
  cache: PublicMonitorCache,
  opts: { assumeNormalized?: boolean } = {},
): MaterializedPublicMonitorCache {
  const normalized = opts.assumeNormalized ? cache : normalizePublicMonitorCache(cache);
  const uptimePctMilli = new Array<number | null>(normalized.uptime_days.day_start_at.length);

  let totalSec = 0;
  let uptimeSec = 0;

  for (let index = 0; index < normalized.uptime_days.day_start_at.length; index += 1) {
    const bucketTotal = normalized.uptime_days.total_sec[index] ?? 0;
    const bucketUptime = normalized.uptime_days.uptime_sec[index] ?? 0;

    totalSec += bucketTotal;
    uptimeSec += bucketUptime;
    uptimePctMilli[index] = bucketTotal <= 0 ? null : Math.round((bucketUptime / bucketTotal) * 100_000);
  }

  return {
    heartbeat_strip: {
      checked_at: normalized.heartbeat.checked_at,
      status_codes: normalized.heartbeat.status_codes,
      latency_ms: normalized.heartbeat.latency_ms,
    },
    uptime_day_strip: {
      day_start_at: normalized.uptime_days.day_start_at,
      downtime_sec: normalized.uptime_days.downtime_sec,
      unknown_sec: normalized.uptime_days.unknown_sec,
      uptime_pct_milli: uptimePctMilli,
    },
    uptime_30d:
      totalSec <= 0
        ? null
        : {
            uptime_pct: (uptimeSec / totalSec) * 100,
          },
  };
}

export async function buildPublicMonitorCacheSeeds(
  db: D1Database,
  now: number,
  rows: PublicMonitorCacheSeedRow[],
): Promise<Map<number, PublicMonitorCache>> {
  const uniqueRows = [...new Map(rows.map((row) => [row.id, row])).values()];
  const out = new Map<number, PublicMonitorCache>();
  if (uniqueRows.length === 0) {
    return out;
  }

  const ids = uniqueRows.map((row) => row.id);
  const placeholders = buildNumberedPlaceholders(ids.length);
  const rangeStart = utcDayStart(now) - (UPTIME_DAYS - 1) * 86_400;
  const todayStartAt = utcDayStart(now);

  const [heartbeatRows, rollupRows, todayByMonitorId] = await Promise.all([
    db
      .prepare(
        `
        SELECT monitor_id, checked_at, status, latency_ms
        FROM (
          SELECT
            id,
            monitor_id,
            checked_at,
            status,
            latency_ms,
            ROW_NUMBER() OVER (
              PARTITION BY monitor_id
              ORDER BY checked_at DESC, id DESC
            ) AS rn
          FROM check_results
          WHERE monitor_id IN (${placeholders})
        )
        WHERE rn <= ?${ids.length + 1}
        ORDER BY monitor_id, checked_at DESC, id DESC
      `,
      )
      .bind(...ids, HEARTBEAT_POINTS)
      .all<HeartbeatQueryRow>()
      .then(({ results }) => results ?? []),
    db
      .prepare(
        `
        SELECT monitor_id, day_start_at, total_sec, downtime_sec, unknown_sec, uptime_sec
        FROM monitor_daily_rollups
        WHERE monitor_id IN (${placeholders})
          AND day_start_at >= ?${ids.length + 1}
          AND day_start_at < ?${ids.length + 2}
        ORDER BY monitor_id, day_start_at
      `,
      )
      .bind(...ids, rangeStart, todayStartAt)
      .all<UptimeRollupRow>()
      .then(({ results }) => results ?? []),
    computeTodayPartialUptimeBatch(
      db,
      uniqueRows,
      Math.max(todayStartAt, rangeStart),
      now,
    ),
  ]);

  const heartbeatsByMonitorId = new Map<number, HeartbeatQueryRow[]>();
  for (const row of heartbeatRows) {
    const existing = heartbeatsByMonitorId.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    heartbeatsByMonitorId.set(row.monitor_id, [row]);
  }

  const rollupsByMonitorId = new Map<number, UptimeRollupRow[]>();
  for (const row of rollupRows) {
    const existing = rollupsByMonitorId.get(row.monitor_id);
    if (existing) {
      existing.push(row);
      continue;
    }
    rollupsByMonitorId.set(row.monitor_id, [row]);
  }

  for (const row of uniqueRows) {
    const historicalDays = (rollupsByMonitorId.get(row.id) ?? []).map((bucket) => ({
      day_start_at: bucket.day_start_at,
      total_sec: bucket.total_sec ?? 0,
      downtime_sec: bucket.downtime_sec ?? 0,
      unknown_sec: bucket.unknown_sec ?? 0,
      uptime_sec: bucket.uptime_sec ?? 0,
    }));
    const today = todayByMonitorId.get(row.id);
    if (today && today.total_sec > 0) {
      historicalDays.push({
        day_start_at: todayStartAt,
        total_sec: today.total_sec,
        downtime_sec: today.downtime_sec,
        unknown_sec: today.unknown_sec,
        uptime_sec: today.uptime_sec,
      });
    }

    out.set(
      row.id,
      buildCacheFromRows({
        heartbeats: heartbeatsByMonitorId.get(row.id) ?? [],
        uptimeDays: historicalDays,
      }),
    );
  }

  return out;
}

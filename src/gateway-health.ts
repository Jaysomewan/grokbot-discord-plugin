import {
  gatewayHeartbeatStaleMs,
  gatewayMinRecoveryIntervalMs,
  gatewayMissedProbesToRecover,
  gatewayNewChannelLiveMs,
  gatewayStallGraceMs,
} from "./constants.ts";

const discordEpochMs = 1_420_074_000_000n;

export type MessageSighting = {
  channelId: string;
  messageId: string;
};

export type GatewayHealthReason = "inbound-stall" | "heartbeat-stale";

export type GatewayHealthDecision = {
  action: "none" | "recover";
  reason: GatewayHealthReason | null;
  missed: MessageSighting[];
};

export type GatewayHealthOptions = {
  stallGraceMs?: number;
  missedProbesToRecover?: number;
  heartbeatStaleMs?: number;
  minRecoveryIntervalMs?: number;
  newChannelLiveMs?: number;
};

type ChannelCursor = {
  seenId: string | null;
};

const noneDecision: GatewayHealthDecision = { action: "none", reason: null, missed: [] };

export function snowflakeTimeMs(id: string): number | null {
  if (!/^\d{17,20}$/.test(id)) {
    return null;
  }

  try {
    return Number((BigInt(id) >> 22n) + discordEpochMs);
  } catch {
    return null;
  }
}

export function snowflakeAt(ms: number): string {
  const value = (BigInt(ms) - discordEpochMs) << 22n;
  return value > 0n ? value.toString() : "0";
}

export function compareSnowflakes(left: string, right: string): number {
  const a = BigInt(left);
  const b = BigInt(right);
  if (a === b) {
    return 0;
  }
  return a > b ? 1 : -1;
}

function laterSnowflake(current: string | null, next: string): string {
  if (!current || compareSnowflakes(next, current) > 0) {
    return next;
  }
  return current;
}

function groupSightings(sightings: MessageSighting[]): Map<string, string[]> {
  const grouped = new Map<string, string[]>();
  for (const sighting of sightings) {
    if (!sighting.channelId || snowflakeTimeMs(sighting.messageId) === null) {
      continue;
    }
    const list = grouped.get(sighting.channelId) ?? [];
    if (!list.includes(sighting.messageId)) {
      list.push(sighting.messageId);
    }
    grouped.set(sighting.channelId, list);
  }

  for (const list of grouped.values()) {
    list.sort(compareSnowflakes);
  }

  return grouped;
}

export function createGatewayHealth(options: GatewayHealthOptions = {}) {
  const stallGraceMs = options.stallGraceMs ?? gatewayStallGraceMs;
  const missedProbesToRecover = options.missedProbesToRecover ?? gatewayMissedProbesToRecover;
  const heartbeatStaleMs = options.heartbeatStaleMs ?? gatewayHeartbeatStaleMs;
  const minRecoveryIntervalMs = options.minRecoveryIntervalMs ?? gatewayMinRecoveryIntervalMs;
  const newChannelLiveMs = options.newChannelLiveMs ?? gatewayNewChannelLiveMs;

  const cursors = new Map<string, ChannelCursor>();
  let ready = false;
  let baselining = false;
  let lastReadyAt: number | null = null;
  let lastHeartbeatAt: number | null = null;
  let lastRecoveryAt: number | null = null;
  let consecutiveStalls = 0;

  function cursorFor(channelId: string): ChannelCursor {
    const existing = cursors.get(channelId);
    if (existing) {
      return existing;
    }
    const created = { seenId: null };
    cursors.set(channelId, created);
    return created;
  }

  function coolingDown(now: number): boolean {
    return lastRecoveryAt !== null && now - lastRecoveryAt < minRecoveryIntervalMs;
  }

  function heartbeatIsStale(now: number): boolean {
    if (!ready || lastReadyAt === null) {
      return false;
    }
    if (now - lastReadyAt < heartbeatStaleMs) {
      return false;
    }
    if (lastHeartbeatAt === null) {
      return true;
    }
    return now - lastHeartbeatAt >= heartbeatStaleMs;
  }

  function missedSightings(sightings: MessageSighting[], now: number): MessageSighting[] {
    const missed: MessageSighting[] = [];
    for (const [channelId, ids] of groupSightings(sightings)) {
      const cursor = cursorFor(channelId);
      const unseen = ids.filter((id) => !cursor.seenId || compareSnowflakes(id, cursor.seenId) > 0);
      const aged = unseen.filter((id) => {
        const at = snowflakeTimeMs(id);
        return at !== null && now - at > stallGraceMs;
      });
      if (aged.length === 0) {
        continue;
      }

      if (!cursor.seenId) {
        const newest = aged[aged.length - 1];
        const newestAt = newest ? snowflakeTimeMs(newest) : null;
        if (!newest || newestAt === null) {
          continue;
        }
        if (now - newestAt > newChannelLiveMs) {
          cursor.seenId = newest;
          continue;
        }
        for (const id of aged) {
          const at = snowflakeTimeMs(id);
          if (at !== null && now - at <= newChannelLiveMs) {
            missed.push({ channelId, messageId: id });
          }
        }
        continue;
      }

      for (const id of aged) {
        missed.push({ channelId, messageId: id });
      }
    }

    missed.sort((left, right) => compareSnowflakes(left.messageId, right.messageId));
    return missed;
  }

  function recover(reason: GatewayHealthReason, missed: MessageSighting[], now: number): GatewayHealthDecision {
    if (coolingDown(now)) {
      return noneDecision;
    }
    return { action: "recover", reason, missed };
  }

  return {
    isReady() {
      return ready;
    },
    isBaselining() {
      return baselining;
    },
    markReady(now: number, mode: "identify" | "resume") {
      ready = true;
      baselining = mode === "identify";
      lastReadyAt = now;
      lastHeartbeatAt = null;
      consecutiveStalls = 0;
    },
    markDisconnected() {
      ready = false;
      baselining = false;
    },
    markHeartbeat(at: number | null) {
      if (at === null) {
        lastHeartbeatAt = null;
        return;
      }
      // Ignore acks from the session we just replaced. A fresh IDENTIFY
      // has to earn a new ack before the heartbeat is treated as healthy.
      if (lastReadyAt !== null && at < lastReadyAt) {
        return;
      }
      lastHeartbeatAt = at;
    },
    markRecovered(now: number) {
      lastRecoveryAt = now;
      consecutiveStalls = 0;
    },
    markInbound(channelId: string, messageId: string) {
      if (snowflakeTimeMs(messageId) === null) {
        return;
      }
      const cursor = cursorFor(channelId);
      cursor.seenId = laterSnowflake(cursor.seenId, messageId);
      consecutiveStalls = 0;
    },
    adoptBaseline(sightings: MessageSighting[], now: number) {
      // History from before this ready event is not a miss. Messages created
      // after ready stay eligible for stall detection.
      const cutoff = lastReadyAt ?? now;
      for (const [channelId, ids] of groupSightings(sightings)) {
        const cursor = cursorFor(channelId);
        for (const id of ids) {
          const at = snowflakeTimeMs(id);
          if (at !== null && at <= cutoff) {
            cursor.seenId = laterSnowflake(cursor.seenId, id);
          }
        }
      }
    },
    finishBaseline() {
      baselining = false;
    },
    evaluate(sightings: MessageSighting[], now: number, probeOk: boolean): GatewayHealthDecision {
      if (!ready) {
        return noneDecision;
      }

      if (!probeOk) {
        if (heartbeatIsStale(now)) {
          return recover("heartbeat-stale", [], now);
        }
        return noneDecision;
      }

      const missed = missedSightings(sightings, now);
      if (missed.length > 0) {
        consecutiveStalls += 1;
      } else {
        consecutiveStalls = 0;
      }

      if (heartbeatIsStale(now)) {
        return recover("heartbeat-stale", missed, now);
      }

      if (missed.length > 0 && consecutiveStalls >= missedProbesToRecover) {
        return recover("inbound-stall", missed, now);
      }

      return noneDecision;
    },
  };
}

export type GatewayHealth = ReturnType<typeof createGatewayHealth>;

import type { GatewayHealth, GatewayHealthDecision, MessageSighting } from "./gateway-health.ts";

export type GatewayProbeResult = {
  ok: boolean;
  /** Every configured parent channel was readable. Thread listing is best-effort. */
  complete: boolean;
  sightings: MessageSighting[];
};

export type GatewayRecovery = {
  closeReason: string;
  logLine: string;
};

export type GatewayGuardDeps = {
  health: GatewayHealth;
  now: () => number;
  lastHeartbeatAt: () => number | null;
  probe: () => Promise<GatewayProbeResult>;
  catchUp: (missed: MessageSighting[]) => Promise<void>;
  reidentify: (recovery: GatewayRecovery) => Promise<void>;
  onFatal: (error: unknown) => void;
};

const catchUpLimit = 20;

export function createIdClaim(limit = 4000) {
  const seen = new Set<string>();
  const order: string[] = [];

  return {
    claim(id: string): boolean {
      if (!id || seen.has(id)) {
        return false;
      }
      seen.add(id);
      order.push(id);
      while (order.length > limit) {
        const oldest = order.shift();
        if (oldest) {
          seen.delete(oldest);
        }
      }
      return true;
    },
  };
}

export function describeRecovery(decision: GatewayHealthDecision): GatewayRecovery {
  const ids = decision.missed.map((row) => row.messageId);
  const shown = ids.slice(0, 8).join(",");
  const extra = ids.length > 8 ? `,+${ids.length - 8}` : "";
  const missed = ids.length > 0 ? ` missed=${shown}${extra}` : "";

  if (decision.reason === "heartbeat-stale") {
    return {
      closeReason: "heartbeat stale",
      logLine: `gateway heartbeat stale; re-identifying${missed}`,
    };
  }

  return {
    closeReason: "inbound stalled",
    logLine: `gateway inbound stalled; re-identifying${missed}`,
  };
}

function capMissed(missed: MessageSighting[]): MessageSighting[] {
  if (missed.length <= catchUpLimit) {
    return missed;
  }
  return missed.slice(missed.length - catchUpLimit);
}

export async function runGatewayGuardTick(deps: GatewayGuardDeps): Promise<void> {
  const { health } = deps;
  if (!health.isReady()) {
    return;
  }

  health.markHeartbeat(deps.lastHeartbeatAt());
  const now = deps.now();
  let probeOk = false;
  let sightings: MessageSighting[] = [];

  try {
    const probe = await deps.probe();
    probeOk = probe.ok;
    sightings = probe.sightings;
    if (health.isBaselining() && probe.ok) {
      health.adoptBaseline(sightings, now);
      if (probe.complete) {
        health.finishBaseline();
      }
    }
  } catch {
    probeOk = false;
    sightings = [];
  }

  const decision = health.evaluate(sightings, deps.now(), probeOk);
  if (decision.action !== "recover" || !decision.reason) {
    return;
  }

  const missed = capMissed(decision.missed);
  const recovery = describeRecovery({ ...decision, missed });
  health.markRecovered(deps.now());
  try {
    await deps.catchUp(missed);
  } catch {
    // A failed catch-up leaves the cursor unchanged so a later probe can retry.
  }

  try {
    await deps.reidentify(recovery);
  } catch (error) {
    deps.onFatal(error);
  }
}

export function createGatewayGuard(deps: GatewayGuardDeps) {
  let running = false;

  return {
    async tick(): Promise<void> {
      if (running) {
        return;
      }
      running = true;
      try {
        await runGatewayGuardTick(deps);
      } finally {
        running = false;
      }
    },
  };
}

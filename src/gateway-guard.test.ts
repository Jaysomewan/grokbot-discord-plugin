import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { createGatewayHealth, snowflakeAt, type MessageSighting } from "./gateway-health.ts";
import { createGatewayGuard, createIdClaim, runGatewayGuardTick, type GatewayProbeResult } from "./gateway-guard.ts";

function setup() {
  const health = createGatewayHealth({
    stallGraceMs: 1_000,
    missedProbesToRecover: 2,
    heartbeatStaleMs: 5_000,
    minRecoveryIntervalMs: 10_000,
    newChannelLiveMs: 60_000,
  });
  let now = 1_700_000_000_000;
  const order: string[] = [];
  let probe: GatewayProbeResult = { ok: true, complete: true, sightings: [] };
  let heartbeatAt: number | null = now;
  const deps = {
    health,
    now: () => now,
    lastHeartbeatAt: () => heartbeatAt,
    async probe() {
      return probe;
    },
    async catchUp(missed: MessageSighting[]) {
      order.push(`catch:${missed.map((row) => row.messageId).join(",")}`);
    },
    async reidentify(recovery: { logLine: string }) {
      order.push(recovery.logLine);
    },
    onFatal(error: unknown) {
      order.push(`fatal:${error instanceof Error ? error.message : "error"}`);
    },
  };

  return {
    health,
    deps,
    order,
    setNow(value: number) {
      now = value;
    },
    setProbe(next: GatewayProbeResult) {
      probe = next;
    },
    setHeartbeat(value: number | null) {
      heartbeatAt = value;
    },
  };
}

describe("gateway guard", () => {
  it("baselines pre-ready history without re-identifying", async () => {
    const ctx = setup();
    const readyAt = 1_700_000_000_000;
    ctx.health.markReady(readyAt, "identify");
    ctx.setNow(readyAt + 2_000);
    ctx.setHeartbeat(readyAt + 1_000);
    const historical = snowflakeAt(readyAt - 120_000);
    ctx.setProbe({
      ok: true,
      complete: true,
      sightings: [{ channelId: "222222222222222222", messageId: historical }],
    });

    await runGatewayGuardTick(ctx.deps);
    assert.deepEqual(ctx.order, []);
    assert.equal(ctx.health.isBaselining(), false);
    assert.equal(ctx.health.isReady(), true);
  });

  it("catch-up posts missed messages before re-identifying", async () => {
    const ctx = setup();
    const readyAt = 1_700_000_000_000;
    ctx.health.markReady(readyAt, "identify");
    ctx.health.finishBaseline();
    ctx.setHeartbeat(readyAt + 100);
    const messageId = snowflakeAt(readyAt + 1_000);
    ctx.setProbe({
      ok: true,
      complete: true,
      sightings: [{ channelId: "222222222222222222", messageId }],
    });

    ctx.setNow(readyAt + 3_000);
    await runGatewayGuardTick(ctx.deps);
    assert.deepEqual(ctx.order, []);

    ctx.setNow(readyAt + 5_000);
    await runGatewayGuardTick(ctx.deps);
    assert.equal(ctx.order.length, 2);
    assert.equal(ctx.order[0], `catch:${messageId}`);
    assert.match(ctx.order[1] ?? "", /gateway inbound stalled; re-identifying missed=/);
  });

  it("re-identifies when heartbeat acks go stale", async () => {
    const ctx = setup();
    const readyAt = 1_700_000_000_000;
    ctx.health.markReady(readyAt, "identify");
    ctx.health.finishBaseline();
    ctx.setHeartbeat(readyAt + 100);
    ctx.setNow(readyAt + 6_000);
    await runGatewayGuardTick(ctx.deps);
    assert.equal(ctx.order.length, 2);
    assert.equal(ctx.order[0], "catch:");
    assert.match(ctx.order[1] ?? "", /^gateway heartbeat stale; re-identifying$/);
  });

  it("reports a fatal re-identify after catch-up", async () => {
    const ctx = setup();
    const readyAt = 1_700_000_000_000;
    ctx.health.markReady(readyAt, "identify");
    ctx.health.finishBaseline();
    ctx.setHeartbeat(readyAt + 100);
    ctx.setNow(readyAt + 6_000);
    ctx.deps.reidentify = async () => {
      throw new Error("socket gone");
    };
    await runGatewayGuardTick(ctx.deps);
    assert.deepEqual(ctx.order, ["catch:", "fatal:socket gone"]);
  });

  it("skips overlapping ticks", async () => {
    const ctx = setup();
    let started = 0;
    let release: (() => void) | undefined;
    ctx.health.markReady(1_700_000_000_000, "resume");
    ctx.deps.probe = () => {
      started += 1;
      return new Promise((resolve) => {
        release = () => resolve({ ok: true, complete: true, sightings: [] });
      });
    };
    const guard = createGatewayGuard(ctx.deps);
    const first = guard.tick();
    const second = guard.tick();
    assert.equal(started, 1);
    release?.();
    await first;
    await second;
    assert.equal(started, 1);
  });
});

describe("inbound claim", () => {
  it("accepts an id once until the ring evicts it", () => {
    const claims = createIdClaim(2);
    assert.equal(claims.claim("a"), true);
    assert.equal(claims.claim("a"), false);
    assert.equal(claims.claim("b"), true);
    assert.equal(claims.claim("c"), true);
    assert.equal(claims.claim("a"), true);
  });
});

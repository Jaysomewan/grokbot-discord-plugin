import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  compareSnowflakes,
  createGatewayHealth,
  snowflakeAt,
  snowflakeTimeMs,
} from "./gateway-health.ts";

const channelId = "222222222222222222";

function health(overrides: Parameters<typeof createGatewayHealth>[0] = {}) {
  return createGatewayHealth({
    stallGraceMs: 1_000,
    missedProbesToRecover: 2,
    heartbeatStaleMs: 1_000_000,
    minRecoveryIntervalMs: 10_000,
    newChannelLiveMs: 60_000,
    ...overrides,
  });
}

describe("snowflake helpers", () => {
  it("round-trips a timestamp through a snowflake", () => {
    const ms = 1_700_000_000_000;
    const id = snowflakeAt(ms);
    assert.equal(snowflakeTimeMs(id), ms);
    assert.equal(compareSnowflakes(snowflakeAt(ms + 1_000), id), 1);
  });
});

describe("gateway health", () => {
  it("does not recover before the session is ready", () => {
    const monitor = health();
    const now = 1_700_000_000_000;
    const decision = monitor.evaluate(
      [{ channelId, messageId: snowflakeAt(now - 5_000) }],
      now,
      true,
    );
    assert.equal(decision.action, "none");
  });

  it("adopts pre-ready history and ignores it", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    const historical = snowflakeAt(readyAt - 60_000);
    monitor.adoptBaseline([{ channelId, messageId: historical }], readyAt + 1_000);
    monitor.finishBaseline();

    const decision = monitor.evaluate([{ channelId, messageId: historical }], readyAt + 5_000, true);
    assert.equal(decision.action, "none");
    assert.deepEqual(decision.missed, []);
  });

  it("keeps a post-ready message inside the grace window", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.finishBaseline();
    const messageId = snowflakeAt(readyAt + 100);
    const decision = monitor.evaluate([{ channelId, messageId }], readyAt + 500, true);
    assert.equal(decision.action, "none");
  });

  it("recovers after repeated probes miss a message the gateway never emitted", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.finishBaseline();
    const messageId = snowflakeAt(readyAt + 1_000);
    const first = monitor.evaluate([{ channelId, messageId }], readyAt + 3_000, true);
    assert.equal(first.action, "none");

    const second = monitor.evaluate([{ channelId, messageId }], readyAt + 5_000, true);
    assert.equal(second.action, "recover");
    assert.equal(second.reason, "inbound-stall");
    assert.deepEqual(second.missed, [{ channelId, messageId }]);
  });

  it("treats a gateway event as proof the session is still delivering", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.finishBaseline();
    const messageId = snowflakeAt(readyAt + 1_000);
    assert.equal(monitor.evaluate([{ channelId, messageId }], readyAt + 3_000, true).action, "none");
    monitor.markInbound(channelId, messageId);
    const follow = monitor.evaluate([{ channelId, messageId }], readyAt + 5_000, true);
    assert.equal(follow.action, "none");
  });

  it("adopts an old thread the gateway was never cursored for", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "resume");
    const ancient = snowflakeAt(readyAt - 10 * 60_000);
    const decision = monitor.evaluate([{ channelId, messageId: ancient }], readyAt, true);
    assert.equal(decision.action, "none");
    const again = monitor.evaluate([{ channelId, messageId: ancient }], readyAt + 5_000, true);
    assert.equal(again.action, "none");
  });

  it("recovers when heartbeat acks stop while the session still looks ready", () => {
    const monitor = health({ heartbeatStaleMs: 5_000 });
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.finishBaseline();
    monitor.markHeartbeat(readyAt + 100);
    const decision = monitor.evaluate([], readyAt + 6_000, true);
    assert.equal(decision.action, "recover");
    assert.equal(decision.reason, "heartbeat-stale");
  });

  it("ignores a heartbeat ack from the session that was just replaced", () => {
    const monitor = health({ heartbeatStaleMs: 5_000 });
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.markHeartbeat(readyAt - 50_000);
    const decision = monitor.evaluate([], readyAt + 1_000, true);
    assert.equal(decision.action, "none");
  });

  it("does not let a failed probe erase stall progress", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.finishBaseline();
    const messageId = snowflakeAt(readyAt + 1_000);
    assert.equal(monitor.evaluate([{ channelId, messageId }], readyAt + 3_000, true).action, "none");
    assert.equal(monitor.evaluate([], readyAt + 4_000, false).action, "none");
    const recovered = monitor.evaluate([{ channelId, messageId }], readyAt + 5_000, true);
    assert.equal(recovered.action, "recover");
    assert.equal(recovered.reason, "inbound-stall");
  });

  it("waits out the recovery cooldown", () => {
    const monitor = health();
    const readyAt = 1_700_000_000_000;
    monitor.markReady(readyAt, "identify");
    monitor.finishBaseline();
    const messageId = snowflakeAt(readyAt + 1_000);
    monitor.evaluate([{ channelId, messageId }], readyAt + 3_000, true);
    const first = monitor.evaluate([{ channelId, messageId }], readyAt + 5_000, true);
    assert.equal(first.action, "recover");
    monitor.markRecovered(readyAt + 5_000);
    const cooled = monitor.evaluate([{ channelId, messageId }], readyAt + 8_000, true);
    assert.equal(cooled.action, "none");
    const later = monitor.evaluate([{ channelId, messageId }], readyAt + 16_000, true);
    assert.equal(later.action, "recover");
  });
});

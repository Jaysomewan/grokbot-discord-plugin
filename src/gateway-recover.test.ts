import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { WebSocketShardDestroyRecovery } from "@discordjs/ws";

import { listGatewayShards, reidentifyGatewayShards, type ReidentifyShard } from "./gateway-recover.ts";

describe("reidentifyGatewayShards", () => {
  it("clears the session and identifies again", async () => {
    const calls: Array<{ recover?: number; reason?: string }> = [];
    const shard: ReidentifyShard = {
      async destroy(options) {
        calls.push(options);
      },
    };

    await reidentifyGatewayShards([shard], "inbound stalled");
    assert.equal(calls.length, 1);
    assert.equal(calls[0]?.recover, WebSocketShardDestroyRecovery.Reconnect);
    assert.notEqual(calls[0]?.recover, WebSocketShardDestroyRecovery.Resume);
    assert.equal(calls[0]?.reason, "inbound stalled");
  });

  it("fails when there is no shard to recover", async () => {
    await assert.rejects(reidentifyGatewayShards([], "heartbeat stale"), /no gateway shards/);
  });

  it("reads shards from the discord.js gateway strategy", () => {
    const shard: ReidentifyShard = { async destroy() {} };
    const client = {
      ws: {
        _ws: {
          strategy: {
            shards: {
              values() {
                return [shard];
              },
            },
          },
        },
      },
    };

    assert.deepEqual(listGatewayShards(client), [shard]);
    assert.deepEqual(listGatewayShards({ ws: {} }), []);
  });
});

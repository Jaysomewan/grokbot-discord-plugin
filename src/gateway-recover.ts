import { WebSocketShardDestroyRecovery } from "@discordjs/ws";

export type ReidentifyShard = {
  destroy(options: { recover?: WebSocketShardDestroyRecovery; reason?: string }): Promise<void>;
};

type ShardCollection = {
  values(): Iterable<ReidentifyShard>;
};

export function listGatewayShards(client: { ws: object }): ReidentifyShard[] {
  const ws = client.ws as {
    _ws?: {
      strategy?: {
        shards?: ShardCollection;
      };
    };
  };
  const shards = ws._ws?.strategy?.shards;
  if (!shards || typeof shards.values !== "function") {
    return [];
  }
  return [...shards.values()];
}

/**
 * Drop the current gateway session and IDENTIFY a new one.
 * Resume keeps the session Discord may already have stopped dispatching to.
 */
export async function reidentifyGatewayShards(shards: Iterable<ReidentifyShard>, reason: string): Promise<void> {
  const list = [...shards];
  if (list.length === 0) {
    throw new Error("no gateway shards to re-identify");
  }

  for (const shard of list) {
    await shard.destroy({
      recover: WebSocketShardDestroyRecovery.Reconnect,
      reason,
    });
  }
}

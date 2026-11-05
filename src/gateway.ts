import {
  ChannelType,
  Client,
  GatewayIntentBits,
  Message,
  type GuildTextBasedChannel,
} from "discord.js";

import type { ConnectorConfig } from "./config.ts";
import {
  followUpWindowMs,
  gatewayProbeIntervalMs,
  gatewayProbeMessageLimit,
  publicError,
} from "./constants.ts";
import { conversationKey, isDirectedAtBot, memberHasAllowedRole, shouldHandle } from "./filters.ts";
import { createFileFollowUpStore } from "./follow-up.ts";
import { createGatewayHealth } from "./gateway-health.ts";
import { createGatewayGuard, createIdClaim } from "./gateway-guard.ts";
import { listGatewayShards, reidentifyGatewayShards } from "./gateway-recover.ts";
import { isSnowflake } from "./ids.ts";
import { createChannelMutex } from "./mutex.ts";
import { deliverOutbound } from "./outbound.ts";
import { createRateLimiter } from "./rate-limit.ts";
import { parseRoomCommand } from "./room-commands.ts";
import { defaultRuntimeDir, tokenFingerprint } from "./supervisor.ts";
import { postGrokWebhookResult } from "./webhook.ts";
import { createFileWebhookLedger, createWebhookDeliverer, webhookLedgerPath } from "./webhook-delivery.ts";

function parentChannelIdFromMessage(message: Message): string | null {
  const { channel } = message;
  if (channel.isThread()) {
    return channel.parentId;
  }
  return channel.id;
}

function isSendableParent(channel: unknown): channel is GuildTextBasedChannel {
  if (!channel || typeof channel !== "object" || !("type" in channel)) {
    return false;
  }

  const type = (channel as { type: ChannelType }).type;
  return type === ChannelType.GuildText || type === ChannelType.GuildAnnouncement;
}

async function homeChannel(message: Message): Promise<GuildTextBasedChannel> {
  if (message.channel.isThread()) {
    let parent: GuildTextBasedChannel | null = isSendableParent(message.channel.parent)
      ? message.channel.parent
      : null;

    if (!parent && message.channel.parentId) {
      const fetched = await message.client.channels.fetch(message.channel.parentId);
      if (isSendableParent(fetched)) {
        parent = fetched;
      }
    }

    if (!parent) {
      throw new Error("Cannot find the parent channel for this thread");
    }

    return parent;
  }

  if (!isSendableParent(message.channel)) {
    throw new Error("Only server text channels are allowed");
  }

  return message.channel;
}

function withTimeout<T>(work: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${label} timed out`));
    }, ms);
    timer.unref();
  });
  // A timed-out turn may still settle later. Swallow that rejection so it
  // cannot crash the process after the caller has moved on.
  work.catch(() => undefined);
  return Promise.race([work, timeout]).finally(() => {
    if (timer) {
      clearTimeout(timer);
    }
  });
}

function warnThrottled(gate: Map<string, number>, key: string, line: string): void {
  const now = Date.now();
  const previous = gate.get(key) ?? 0;
  if (now - previous < 5 * 60 * 1000) {
    return;
  }
  gate.set(key, now);
  process.stderr.write(`grok-discord ${line}\n`);
}

function isMissingResource(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }
  const record = error as { code?: unknown; status?: unknown; message?: unknown };
  if (record.code === 10003 || record.code === 10008 || record.status === 404) {
    return true;
  }
  return typeof record.message === "string" && record.message.includes("Unknown Message");
}

function botNamesFor(config: ConnectorConfig, client: Client): string[] {
  const names = [config.botNamePrefix, client.user?.username, client.user?.globalName];
  return [...new Set(names.filter((name): name is string => Boolean(name && name.trim())))];
}

async function isReplyToBot(client: Client, message: Message): Promise<boolean> {
  if (!client.user) {
    return false;
  }

  if (message.mentions.repliedUser?.id === client.user.id) {
    return true;
  }

  const refId = message.reference?.messageId;
  if (!refId || !("messages" in message.channel)) {
    return false;
  }

  const referenced =
    message.channel.messages.cache.get(refId) ??
    (await message.channel.messages.fetch(refId).catch(() => null));

  return referenced?.author.id === client.user.id;
}

async function speak(
  config: ConnectorConfig,
  message: Message,
  text: string,
  replyTo = true,
): Promise<void> {
  const parentId = parentChannelIdFromMessage(message);
  const home = await homeChannel(message);
  const inThread = message.channel.isThread();

  await deliverOutbound({
    config,
    guildId: message.guildId,
    parentChannelId: parentId,
    inThread,
    replyToMessageId: replyTo ? message.id : undefined,
    text,
    async post(chunk, mode) {
      if (mode === "reply") {
        await message.reply({
          content: chunk,
          allowedMentions: { parse: [], repliedUser: false },
        });
        return;
      }

      await home.send({ content: chunk, allowedMentions: { parse: [] } });
    },
  });
}

export async function startDiscordGateway(config: ConnectorConfig): Promise<Client> {
  const runtimeDir = defaultRuntimeDir();
  const fingerprint = tokenFingerprint(config.discordToken);
  const followUps = createFileFollowUpStore(runtimeDir, fingerprint, followUpWindowMs);
  const mutex = createChannelMutex();
  const userRate = createRateLimiter(config.rateWindowMs, config.ratePerUser);
  const channelRate = createRateLimiter(config.rateWindowMs, config.ratePerChannel);
  const health = createGatewayHealth();
  const claims = createIdClaim();
  const warnGate = new Map<string, number>();
  const deliverer = createWebhookDeliverer(createFileWebhookLedger(webhookLedgerPath(runtimeDir, fingerprint)), {
    now: () => Date.now(),
    log(line) {
      process.stderr.write(`grok-discord ${line}\n`);
    },
    async post(payload) {
      if (!config.grokWebhookUrl) {
        return { ok: false, error: "GROK_WEBHOOK_URL is not set" };
      }
      return postGrokWebhookResult(config.grokWebhookUrl, payload, config.grokWebhookSecret);
    },
  });

  const client = new Client({
    intents: [
      GatewayIntentBits.Guilds,
      GatewayIntentBits.GuildMessages,
      GatewayIntentBits.MessageContent,
    ],
  });

  let bootLogged = false;
  const logBootReady = () => {
    if (bootLogged) {
      return;
    }
    bootLogged = true;
    const tag = client.user?.tag ?? "unknown";
    process.stdout.write(`grok-discord ready as ${tag}; one replica per bot token\n`);
    if (!config.grokWebhookUrl) {
      process.stdout.write("grok-discord GROK_WEBHOOK_URL is not set; inbound will not wake a Grok Bot\n");
    }
  };

  client.on("clientReady", logBootReady);
  client.on("shardReady", (shardId: number) => {
    const first = !bootLogged;
    logBootReady();
    health.markReady(Date.now(), "identify");
    if (!first) {
      process.stdout.write(`grok-discord gateway session ready shard=${shardId}\n`);
    }
  });
  client.on("shardResume", (shardId: number, replayed: number) => {
    health.markReady(Date.now(), "resume");
    process.stdout.write(`grok-discord gateway session resumed shard=${shardId} replayed=${replayed}\n`);
  });
  client.on("shardReconnecting", (shardId: number) => {
    health.markDisconnected();
    process.stderr.write(`grok-discord gateway reconnecting shard=${shardId}\n`);
  });
  client.on("shardDisconnect", (event: { code?: number }, shardId: number) => {
    health.markDisconnected();
    process.stderr.write(`grok-discord gateway disconnect shard=${shardId} code=${event?.code ?? "unknown"}\n`);
  });
  client.on("shardError", (error: Error, shardId: number) => {
    process.stderr.write(`grok-discord gateway shard error shard=${shardId}: ${error.message}\n`);
  });
  client.on("error", (error: Error) => {
    process.stderr.write(`grok-discord gateway error: ${error.message}\n`);
  });

  async function fetchRecentMessageIds(channelId: string): Promise<string[]> {
    // REST, not channel.messages.fetch. discord.js drops messageCreate when a
    // non-bot message is already in the cache, which would hide a healthy gateway.
    const raw = await client.rest.get(`/channels/${channelId}/messages`, {
      query: new URLSearchParams({ limit: String(gatewayProbeMessageLimit) }),
    });
    if (!Array.isArray(raw)) {
      return [];
    }

    const ids: string[] = [];
    for (const row of raw) {
      if (!row || typeof row !== "object") {
        continue;
      }
      const message = row as { id?: unknown; webhook_id?: unknown; author?: { bot?: unknown } };
      if (message.webhook_id || message.author?.bot === true) {
        continue;
      }
      const id = typeof message.id === "string" ? message.id : "";
      if (isSnowflake(id)) {
        ids.push(id);
      }
    }
    return ids;
  }

  async function fetchActiveThreadIds(): Promise<string[]> {
    const raw = await client.rest.get(`/guilds/${config.discordGuildId}/threads/active`);
    if (!raw || typeof raw !== "object" || !("threads" in raw) || !Array.isArray(raw.threads)) {
      return [];
    }

    const ids: string[] = [];
    for (const row of raw.threads) {
      if (!row || typeof row !== "object") {
        continue;
      }
      const id = "id" in row && typeof row.id === "string" ? row.id : "";
      const parentId = "parent_id" in row && typeof row.parent_id === "string" ? row.parent_id : "";
      if (!isSnowflake(id) || !config.discordChannelIds.includes(parentId)) {
        continue;
      }
      ids.push(id);
    }
    return ids;
  }

  async function ingestMessage(message: Message): Promise<void> {
    // Cursor advances only when messageCreate actually ran. A ready socket that
    // stops emitting events leaves this cursor behind REST history.
    health.markInbound(message.channel.id, message.id);
    if (!claims.claim(message.id)) {
      return;
    }

    const parentId = parentChannelIdFromMessage(message);
    if (
      !shouldHandle({
        authorIsBot: message.author.bot,
        webhookId: message.webhookId,
        guildId: message.guildId,
        parentChannelId: parentId,
        configuredGuildId: config.discordGuildId,
        configuredChannelIds: config.discordChannelIds,
      })
    ) {
      return;
    }

    const guildId = message.guildId;
    if (!parentId || !guildId) {
      return;
    }

    const names = botNamesFor(config, client);
    const sessionKey = conversationKey(parentId, message.author.id);
    const directed = isDirectedAtBot({
      mentionedBot: Boolean(client.user && message.mentions.has(client.user)),
      isReplyToBot: await withTimeout(isReplyToBot(client, message), 5_000, "reply lookup").catch(() => false),
      content: message.cleanContent,
      botNames: names,
      followUp: followUps.isFollowUp(sessionKey),
    });

    if (!directed) {
      return;
    }

    const member = message.member;
    const roleIds = member ? [...member.roles.cache.keys()] : null;
    if (!memberHasAllowedRole(config.discordAllowedRoleIds, roleIds)) {
      return;
    }

    process.stdout.write(
      `grok-discord inbound directed message_id=${message.id} channel_id=${message.channel.id}\n`,
    );

    await mutex.run(parentId, async () => {
      try {
        await withTimeout(
          (async () => {
            if (!userRate.allow(message.author.id) || !channelRate.allow(parentId)) {
              process.stderr.write("grok-discord inbound dropped: rate limited\n");
              return;
            }

            const roomCommand = parseRoomCommand(message.cleanContent, names);
            if (roomCommand?.kind === "new") {
              followUps.clear(sessionKey);
              await speak(config, message, "Fresh thread. I'm listening.");
              return;
            }

            if (roomCommand?.kind === "status") {
              await speak(config, message, "Yeah, I'm here. Gateway up.");
              return;
            }

            if (!config.grokWebhookUrl) {
              process.stdout.write("grok-discord inbound dropped: GROK_WEBHOOK_URL is not set\n");
              return;
            }

            const status = await deliverer.deliver({
              message_id: message.id,
              channel_id: message.channel.id,
              guild_id: guildId,
              parent_channel_id: parentId,
              author_id: message.author.id,
              author_username: message.author.username,
              content: message.cleanContent,
              is_reply: Boolean(message.reference?.messageId),
              referenced_message_id: message.reference?.messageId ?? null,
            });

            if (status === "failed") {
              await speak(config, message, publicError);
            }
          })(),
          25_000,
          "inbound",
        );
      } catch (error) {
        const detail = error instanceof Error ? error.message : "unknown error";
        process.stderr.write(`grok-discord turn error: ${detail}\n`);
        try {
          await speak(config, message, publicError);
        } catch (speakError) {
          const speakDetail = speakError instanceof Error ? speakError.message : "speak failed";
          process.stderr.write(`grok-discord public error failed: ${speakDetail}\n`);
        }
      }
    });
  }

  client.on("messageCreate", (message) => {
    void ingestMessage(message).catch((error: unknown) => {
      const detail = error instanceof Error ? error.message : "unknown error";
      process.stderr.write(`grok-discord turn error: ${detail}\n`);
    });
  });

  const guard = createGatewayGuard({
    health,
    now: () => Date.now(),
    lastHeartbeatAt() {
      let newest: number | null = null;
      for (const shard of client.ws.shards.values()) {
        const stamp = shard.lastPingTimestamp;
        if (typeof stamp === "number" && stamp > 0 && (newest === null || stamp > newest)) {
          newest = stamp;
        }
      }
      return newest;
    },
    async probe() {
      const sightings: Array<{ channelId: string; messageId: string }> = [];
      let failedParents = 0;
      for (const channelId of config.discordChannelIds) {
        try {
          for (const messageId of await fetchRecentMessageIds(channelId)) {
            sightings.push({ channelId, messageId });
          }
        } catch (error) {
          failedParents += 1;
          const detail = error instanceof Error ? error.message : "probe failed";
          warnThrottled(warnGate, `probe:${channelId}`, `gateway probe failed channel_id=${channelId}: ${detail}`);
        }
      }

      try {
        const threadIds = await fetchActiveThreadIds();
        for (const threadId of threadIds.slice(0, 20)) {
          try {
            for (const messageId of await fetchRecentMessageIds(threadId)) {
              sightings.push({ channelId: threadId, messageId });
            }
          } catch {
            // A thread the bot cannot read is not evidence the parent gateway stalled.
          }
        }
      } catch (error) {
        const detail = error instanceof Error ? error.message : "thread probe failed";
        warnThrottled(warnGate, "probe:threads", `gateway thread probe failed: ${detail}`);
      }

      return {
        ok: config.discordChannelIds.length === 0 || failedParents < config.discordChannelIds.length,
        complete: failedParents === 0,
        sightings,
      };
    },
    async catchUp(missed) {
      for (const row of missed) {
        try {
          const channel = await client.channels.fetch(row.channelId);
          if (!channel || !channel.isTextBased() || !("messages" in channel)) {
            health.markInbound(row.channelId, row.messageId);
            continue;
          }
          const message = await channel.messages.fetch(row.messageId);
          await ingestMessage(message);
        } catch (error) {
          if (isMissingResource(error)) {
            health.markInbound(row.channelId, row.messageId);
            continue;
          }
          const detail = error instanceof Error ? error.message : "catch-up failed";
          process.stderr.write(`grok-discord gateway catch-up failed message_id=${row.messageId}: ${detail}\n`);
        }
      }
    },
    async reidentify(recovery) {
      process.stderr.write(`grok-discord ${recovery.logLine}\n`);
      health.markDisconnected();
      await withTimeout(
        reidentifyGatewayShards(listGatewayShards(client), recovery.closeReason),
        90_000,
        "re-identify",
      );
    },
    onFatal(error) {
      const detail = error instanceof Error ? error.message : "re-identify failed";
      process.stderr.write(`grok-discord gateway re-identify failed: ${detail}\n`);
      process.exit(1);
    },
  });

  const probeTimer = setInterval(() => {
    void guard.tick();
  }, gatewayProbeIntervalMs);
  probeTimer.unref();

  const pumpTimer = setInterval(() => {
    void deliverer.pump().catch((error: unknown) => {
      const detail = error instanceof Error ? error.message : "webhook pump failed";
      process.stderr.write(`grok-discord ${detail}\n`);
    });
  }, 1_000);
  pumpTimer.unref();

  await client.login(config.discordToken);
  return client;
}

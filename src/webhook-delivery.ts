import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

import { webhookDeliveredTtlMs, webhookRetryBaseMs, webhookRetryMaxDelayMs } from "./constants.ts";
import { redactSecrets } from "./redact.ts";
import { sanitizeWebhookPayload, type WebhookPayload, type WebhookPostResult } from "./webhook.ts";

export type DeliveryStatus = "delivered" | "duplicate" | "pending" | "failed";

type LedgerState = "pending" | "delivered" | "failed";

type LedgerEntry = {
  messageId: string;
  state: LedgerState;
  payload: WebhookPayload;
  attempts: number;
  nextAttemptAt: number;
  updatedAt: number;
  lastError?: string;
};

export type WebhookLedger = {
  get(messageId: string): LedgerEntry | undefined;
  put(entry: LedgerEntry): void;
  listPending(now: number): LedgerEntry[];
};

export function retryDelayMs(attempts: number, baseMs = webhookRetryBaseMs, maxMs = webhookRetryMaxDelayMs): number {
  const exponent = Math.min(10, Math.max(0, attempts - 1));
  return Math.min(maxMs, baseMs * 2 ** exponent);
}

export function classifyWebhookFailure(error: string, status?: number): { retryable: boolean; error: string } {
  if (status === 0 || status === 408 || status === 425 || status === 429 || (status !== undefined && status >= 500)) {
    return { retryable: true, error: status ? `webhook ${status}` : error };
  }

  if (status !== undefined && status >= 400 && status < 500) {
    return { retryable: false, error: `webhook ${status}` };
  }

  if (/GROK_WEBHOOK_SECRET|GROK_WEBHOOK_URL|not allowed/.test(error)) {
    return { retryable: false, error };
  }

  return { retryable: true, error };
}

const maxTrackedEntries = 2000;

function pruneEntries(entries: Map<string, LedgerEntry>, now: number): void {
  for (const [id, entry] of entries) {
    if (entry.state !== "pending" && now - entry.updatedAt >= webhookDeliveredTtlMs) {
      entries.delete(id);
    }
  }

  const terminal = [...entries.values()]
    .filter((entry) => entry.state !== "pending")
    .sort((left, right) => left.updatedAt - right.updatedAt);

  while (entries.size > maxTrackedEntries && terminal.length > 0) {
    const oldest = terminal.shift();
    if (oldest) {
      entries.delete(oldest.messageId);
    }
  }
}

export function createMemoryWebhookLedger(): WebhookLedger {
  const entries = new Map<string, LedgerEntry>();

  return {
    get(messageId) {
      return entries.get(messageId);
    },
    put(entry) {
      entries.set(entry.messageId, entry);
      pruneEntries(entries, entry.updatedAt);
    },
    listPending(now) {
      return [...entries.values()]
        .filter((entry) => entry.state === "pending" && entry.nextAttemptAt <= now)
        .sort((left, right) => left.nextAttemptAt - right.nextAttemptAt);
    },
  };
}

type LedgerFile = {
  entries: LedgerEntry[];
};

function isPayload(value: unknown): value is WebhookPayload {
  if (!value || typeof value !== "object") {
    return false;
  }
  const payload = value as Partial<WebhookPayload>;
  return (
    typeof payload.message_id === "string" &&
    typeof payload.channel_id === "string" &&
    typeof payload.guild_id === "string" &&
    typeof payload.parent_channel_id === "string" &&
    typeof payload.author_id === "string" &&
    typeof payload.author_username === "string" &&
    typeof payload.content === "string" &&
    typeof payload.is_reply === "boolean" &&
    (payload.referenced_message_id === null || typeof payload.referenced_message_id === "string")
  );
}

function readLedgerFile(path: string): Map<string, LedgerEntry> {
  const entries = new Map<string, LedgerEntry>();
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return entries;
  }

  const rows = parsed && typeof parsed === "object" && Array.isArray((parsed as LedgerFile).entries)
    ? (parsed as LedgerFile).entries
    : [];

  for (const row of rows) {
    if (!row || typeof row.messageId !== "string" || !isPayload(row.payload)) {
      continue;
    }
    if (row.state !== "pending" && row.state !== "delivered" && row.state !== "failed") {
      continue;
    }
    entries.set(row.messageId, {
      messageId: row.messageId,
      state: row.state,
      payload: row.payload,
      attempts: Number.isFinite(row.attempts) ? row.attempts : 0,
      nextAttemptAt: Number.isFinite(row.nextAttemptAt) ? row.nextAttemptAt : 0,
      updatedAt: Number.isFinite(row.updatedAt) ? row.updatedAt : 0,
      lastError: typeof row.lastError === "string" ? row.lastError : undefined,
    });
  }

  return entries;
}

function writeLedgerFile(path: string, entries: Map<string, LedgerEntry>): void {
  mkdirSync(dirname(path), { recursive: true });
  const payload: LedgerFile = { entries: [...entries.values()] };
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, `${JSON.stringify(payload)}\n`);
  renameSync(tmp, path);
}

export function webhookLedgerPath(runtimeDir: string, fingerprint: string): string {
  return join(runtimeDir, `webhook-ledger-${fingerprint}.json`);
}

export function createFileWebhookLedger(path: string): WebhookLedger {
  let entries = readLedgerFile(path);

  function persist(now: number) {
    pruneEntries(entries, now);
    try {
      writeLedgerFile(path, entries);
    } catch {
      entries = readLedgerFile(path);
    }
  }

  return {
    get(messageId) {
      return entries.get(messageId);
    },
    put(entry) {
      entries.set(entry.messageId, entry);
      persist(entry.updatedAt);
    },
    listPending(now) {
      return [...entries.values()]
        .filter((entry) => entry.state === "pending" && entry.nextAttemptAt <= now)
        .sort((left, right) => left.nextAttemptAt - right.nextAttemptAt);
    },
  };
}

export type WebhookDelivererDeps = {
  post: (payload: WebhookPayload) => Promise<WebhookPostResult>;
  now: () => number;
  log?: (line: string) => void;
  baseDelayMs?: number;
  maxDelayMs?: number;
};

function safeError(error: string): string {
  return redactSecrets(error).slice(0, 300);
}

export function createWebhookDeliverer(ledger: WebhookLedger, deps: WebhookDelivererDeps) {
  const inflight = new Map<string, Promise<DeliveryStatus>>();
  const baseDelayMs = deps.baseDelayMs ?? webhookRetryBaseMs;
  const maxDelayMs = deps.maxDelayMs ?? webhookRetryMaxDelayMs;

  async function attempt(payload: WebhookPayload): Promise<DeliveryStatus> {
    const safePayload = sanitizeWebhookPayload(payload);
    const existing = ledger.get(safePayload.message_id);
    if (existing?.state === "delivered") {
      return "duplicate";
    }
    if (existing?.state === "failed") {
      return "failed";
    }

    const now = deps.now();
    const attempts = (existing?.attempts ?? 0) + 1;
    ledger.put({
      messageId: safePayload.message_id,
      state: "pending",
      payload: safePayload,
      attempts,
      nextAttemptAt: now,
      updatedAt: now,
    });

    let result: WebhookPostResult;
    try {
      result = await deps.post(safePayload);
    } catch (error) {
      const message = error instanceof Error ? error.message : "webhook failed";
      result = { ok: false, error: message };
    }

    const finishedAt = deps.now();
    if (result.ok) {
      ledger.put({
        messageId: safePayload.message_id,
        state: "delivered",
        payload: safePayload,
        attempts,
        nextAttemptAt: finishedAt,
        updatedAt: finishedAt,
      });
      if (attempts > 1) {
        deps.log?.(`webhook delivered message_id=${safePayload.message_id} attempts=${attempts}`);
      }
      return "delivered";
    }

    const failure = classifyWebhookFailure(result.error, result.status);
    if (!failure.retryable) {
      ledger.put({
        messageId: safePayload.message_id,
        state: "failed",
        payload: safePayload,
        attempts,
        nextAttemptAt: finishedAt,
        updatedAt: finishedAt,
        lastError: failure.error,
      });
      deps.log?.(`webhook failed message_id=${safePayload.message_id} error=${safeError(failure.error)}`);
      return "failed";
    }

    const delay = retryDelayMs(attempts, baseDelayMs, maxDelayMs);
    ledger.put({
      messageId: safePayload.message_id,
      state: "pending",
      payload: safePayload,
      attempts,
      nextAttemptAt: finishedAt + delay,
      updatedAt: finishedAt,
      lastError: failure.error,
    });
    deps.log?.(
      `webhook retry scheduled message_id=${safePayload.message_id} attempt=${attempts} error=${safeError(failure.error)}`,
    );
    return "pending";
  }

  function deliver(payload: WebhookPayload): Promise<DeliveryStatus> {
    const current = inflight.get(payload.message_id);
    if (current) {
      return current;
    }

    const run = attempt(payload).finally(() => {
      if (inflight.get(payload.message_id) === run) {
        inflight.delete(payload.message_id);
      }
    });
    inflight.set(payload.message_id, run);
    return run;
  }

  return {
    deliver,
    async pump(): Promise<void> {
      const due = ledger.listPending(deps.now());
      for (const entry of due) {
        await deliver(entry.payload);
      }
    },
  };
}

export type WebhookDeliverer = ReturnType<typeof createWebhookDeliverer>;

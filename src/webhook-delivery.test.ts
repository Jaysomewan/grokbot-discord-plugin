import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";

import {
  classifyWebhookFailure,
  createFileWebhookLedger,
  createMemoryWebhookLedger,
  createWebhookDeliverer,
  retryDelayMs,
} from "./webhook-delivery.ts";
import type { WebhookPayload } from "./webhook.ts";

const payload: WebhookPayload = {
  message_id: "123",
  channel_id: "2",
  guild_id: "3",
  parent_channel_id: "2",
  author_id: "4",
  author_username: "ada",
  content: "hello",
  is_reply: false,
  referenced_message_id: null,
};

describe("webhook retry policy", () => {
  it("backs off and then caps", () => {
    assert.equal(retryDelayMs(1, 1_000, 60_000), 1_000);
    assert.equal(retryDelayMs(2, 1_000, 60_000), 2_000);
    assert.equal(retryDelayMs(3, 1_000, 60_000), 4_000);
    assert.equal(retryDelayMs(20, 1_000, 60_000), 60_000);
  });

  it("retries transport failures and stops on permanent 4xx", () => {
    assert.equal(classifyWebhookFailure("webhook timeout").retryable, true);
    assert.equal(classifyWebhookFailure("connect ECONNRESET").retryable, true);
    assert.equal(classifyWebhookFailure("webhook 503", 503).retryable, true);
    assert.equal(classifyWebhookFailure("webhook 429", 429).retryable, true);
    assert.equal(classifyWebhookFailure("webhook 400", 400).retryable, false);
    assert.equal(classifyWebhookFailure("GROK_WEBHOOK_URL host is not allowed").retryable, false);
  });
});

describe("webhook deliverer", () => {
  it("redacts secrets before the post is stored or sent", async () => {
    let seen = "";
    const deliverer = createWebhookDeliverer(createMemoryWebhookLedger(), {
      now: () => 1,
      async post(body) {
        seen = body.content;
        return { ok: true, status: 204 };
      },
    });

    await deliverer.deliver({
      ...payload,
      content: "hello ghp_abcdefghijklmnopqrstuvwxyz1234567890",
    });
    assert.equal(seen.includes("ghp_"), false);
    assert.match(seen, /\[redacted\]/);
  });

  it("does not post a message id that already succeeded", async () => {
    let posts = 0;
    let now = 1_000;
    const deliverer = createWebhookDeliverer(createMemoryWebhookLedger(), {
      now: () => now,
      async post() {
        posts += 1;
        return { ok: true, status: 204 };
      },
    });

    assert.equal(await deliverer.deliver(payload), "delivered");
    now += 10_000;
    assert.equal(await deliverer.deliver({ ...payload, content: "ghp_abcdefghijklmnopqrstuvwxyz1234567890" }), "duplicate");
    await deliverer.pump();
    assert.equal(posts, 1);
  });

  it("retries a retryable failure once the backoff is due", async () => {
    const statuses = [503, 204];
    let now = 5_000;
    const deliverer = createWebhookDeliverer(createMemoryWebhookLedger(), {
      now: () => now,
      baseDelayMs: 1_000,
      maxDelayMs: 60_000,
      async post() {
        const status = statuses.shift() ?? 204;
        if (status >= 300) {
          return { ok: false, status, error: `webhook ${status}` };
        }
        return { ok: true, status };
      },
    });

    assert.equal(await deliverer.deliver(payload), "pending");
    now += 500;
    await deliverer.pump();
    assert.equal(statuses.length, 1);

    now += 1_000;
    await deliverer.pump();
    assert.equal(statuses.length, 0);
    assert.equal(await deliverer.deliver(payload), "duplicate");
  });

  it("does not retry a permanent client error", async () => {
    let posts = 0;
    const deliverer = createWebhookDeliverer(createMemoryWebhookLedger(), {
      now: () => 1,
      async post() {
        posts += 1;
        return { ok: false, status: 400, error: "webhook 400" };
      },
    });

    assert.equal(await deliverer.deliver(payload), "failed");
    await deliverer.pump();
    assert.equal(await deliverer.deliver(payload), "failed");
    assert.equal(posts, 1);
  });

  it("single-flights concurrent posts for one message id", async () => {
    let posts = 0;
    let release: (() => void) | undefined;
    const deliverer = createWebhookDeliverer(createMemoryWebhookLedger(), {
      now: () => 1,
      post() {
        posts += 1;
        return new Promise((resolve) => {
          release = () => resolve({ ok: true, status: 204 });
        });
      },
    });

    const first = deliverer.deliver(payload);
    const second = deliverer.deliver(payload);
    assert.equal(posts, 1);
    release?.();
    assert.equal(await first, "delivered");
    assert.equal(await second, "delivered");
    assert.equal(posts, 1);
  });

  it("resumes a pending post from disk without double-sending after 2xx", async () => {
    const dir = mkdtempSync(join(tmpdir(), "grok-discord-ledger-"));
    const path = join(dir, "webhook-ledger.json");
    try {
      let posts = 0;
      let now = 10_000;
      const first = createWebhookDeliverer(createFileWebhookLedger(path), {
        now: () => now,
        baseDelayMs: 1_000,
        async post() {
          posts += 1;
          return { ok: false, status: 502, error: "webhook 502" };
        },
      });
      assert.equal(await first.deliver(payload), "pending");

      const second = createWebhookDeliverer(createFileWebhookLedger(path), {
        now: () => now,
        baseDelayMs: 1_000,
        async post() {
          posts += 1;
          return { ok: true, status: 200 };
        },
      });
      now += 1_000;
      await second.pump();
      await second.pump();
      assert.equal(posts, 2);
      assert.equal(await second.deliver(payload), "duplicate");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

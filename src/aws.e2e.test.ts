import { DeleteObjectCommand, S3Client } from "@aws-sdk/client-s3";
import {
  DeleteMessageBatchCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { describe, it } from "vitest";
import {
  BaseWorkflowState,
  EngineOptions,
  makeS3StateStoreFromEnv,
  makeSqsBrokerFromEnv,
  Processor,
  RunId,
  S3StateStore,
  SqsBroker,
  Workflow,
} from ".";
import {
  continueNow,
  done,
  effectOnce,
  expBackoff,
  fromSteps,
  waitFor,
} from "./states";

function env(name: string): string | undefined {
  const v = process.env[name];
  return v && v.trim() !== "" ? v : undefined;
}

function normalizePrefix(p?: string): string {
  if (!p) return "wf/";
  return p === "" ? "" : p.endsWith("/") ? p : `${p}/`;
}

describe("aws e2e", () => {
  async function drainQueue(sqs: SQSClient, queueUrl: string) {
    for (;;) {
      const r = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
        })
      );
      const msgs = r.Messages ?? [];
      if (msgs.length === 0) return;
      await sqs.send(
        new DeleteMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: msgs.map((m, i) => ({
            Id: String(i),
            ReceiptHandle: m.ReceiptHandle!,
          })),
        })
      );
    }
  }

  it("more complex state machine: initiate, poll, fetch, finish", async () => {
    type UpstreamWorkClient = {
      initiateWork: (runId: string) => Promise<{ jobId: string }>;
      checkWork: (jobId: string) => Promise<"pending" | "ready" | "failed">;
      fetchResult: (jobId: string) => Promise<{ value: string }>;
    };

    const jobs = new Map<
      string,
      { polls: number; readyAfter: number; failed?: boolean }
    >();
    const extra: UpstreamWorkClient = {
      async initiateWork(runId) {
        const jobId = `${runId}-job`;
        jobs.set(jobId, { polls: 0, readyAfter: 2, failed: false });
        return { jobId };
      },
      async checkWork(jobId) {
        const j = jobs.get(jobId);
        if (!j) return "failed";
        j.polls += 1;
        if (j.failed) return "failed";
        return j.polls >= j.readyAfter ? "ready" : "pending";
      },
      async fetchResult(jobId) {
        if (!jobs.has(jobId)) return { value: "missing" };
        return { value: `result:${jobId}` };
      },
    };

    type State = BaseWorkflowState & {
      step: "start" | "poll" | "fetch";
      jobId?: string;
      polls?: number;
      result?: { value: string };
    };

    const wf: Workflow<State> = fromSteps<State>(
      async (runId) => ({
        runId,
        status: "running",
        step: "start",
        polls: 0,
      }),
      {
        start: async (s) => {
          if (!s.jobId) {
            const r = await effectOnce(s, `init:${s.runId}`, () =>
              extra.initiateWork(s.runId)
            );
            if (r) s.jobId = r.jobId;
          }
          s.step = "poll";
          return { state: s, next: waitFor(1000) };
        },
        poll: async (s) => {
          const st = await extra.checkWork(s.jobId!);
          s.polls = (s.polls ?? 0) + 1;
          if (st === "pending")
            return {
              state: s,
              next: waitFor(
                expBackoff({
                  attempt: s.polls ?? 1,
                  baseMs: 1500,
                  maxMs: 30000,
                })
              ),
            };
          if (st === "failed")
            return {
              state: s,
              next: { type: "fail", reason: "upstream failed" },
            };
          s.step = "fetch";
          return { state: s, next: continueNow() };
        },
        fetch: async (s) => {
          const out = await effectOnce(s, `fetch:${s.jobId}`, () =>
            extra.fetchResult(s.jobId!)
          );
          if (out) s.result = out;
          s.status = "done";
          return { state: s, next: done() };
        },
      }
    );

    const runId = `e2e-wf-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2)}` as RunId;
    const s3 = new S3Client({});
    const sqs = new SQSClient({});
    const store: S3StateStore<State> = makeS3StateStoreFromEnv<State>(s3);
    const broker: SqsBroker = makeSqsBrokerFromEnv(sqs);
    const queueUrl = env("HOBO_SQS_URL")!;

    await drainQueue(sqs, queueUrl);

    const engine = new Processor<State>({
      broker,
      store,
    });
    await broker.schedule(runId, { input: { hello: "hobo" } });

    const options: EngineOptions = {
      heartbeatIntervalMs: 500,
      leaseTtlMs: 2000,
      targetVisibilityMs: 30000,
    };
    const recv = async () => {
      const r = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 5,
          WaitTimeSeconds: 10,
        })
      );
      return r.Messages ?? [];
    };

    const deadline = Date.now() + 60000;
    for (;;) {
      const msgs = await recv();
      for (const m of msgs) {
        try {
          const { job, handle } = await broker.normalize(m);
          if (job.runId !== runId) continue;
          await engine.handle(job, handle, wf, options);
        } catch {
          continue;
        }
      }
      const s = await store.load(runId);
      if (s.state?.status === "done") {
        // validate the result shape
        if (!s.state.result || !s.state.result.value) {
          throw new Error("missing result");
        }
        break;
      }
      if (Date.now() > deadline)
        throw new Error("timeout waiting for workflow");
    }

    const bucket = env("HOBO_S3_BUCKET")!;
    const prefix = normalizePrefix(env("HOBO_S3_PREFIX"));
    await s3.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: `${prefix}${runId}` })
    );
  }, 90000);

  it("worker crash: first consumer dies, visibility expires, second completes", async () => {
    type UpstreamWorkClient = {
      doOnce: (runId: string) => Promise<void>;
    };

    const extra: UpstreamWorkClient = {
      async doOnce() {},
    };

    type State = BaseWorkflowState & { step: "start" | "done" };
    const wf: Workflow<State> = fromSteps<State>(
      async (runId) => ({ runId, status: "running", step: "start" }),
      {
        start: async (s) => {
          await extra.doOnce(s.runId);
          s.step = "done";
          s.status = "done";
          return { state: s, next: done() };
        },
        done: async (s) => ({ state: s, next: done() }),
      }
    );

    const runId = `e2e-crash-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2)}` as RunId;
    const s3 = new S3Client({});
    const sqs = new SQSClient({});
    const store: S3StateStore<State> = makeS3StateStoreFromEnv<State>(s3);
    const broker: SqsBroker = makeSqsBrokerFromEnv(sqs);
    const queueUrl = env("HOBO_SQS_URL")!;

    await drainQueue(sqs, queueUrl);

    const fastVisibility = 5; // seconds
    await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        // no effect here; we control via ChangeMessageVisibility on handles
      })
    );

    const processorB = new Processor<State>({
      broker,
      store,
    });
    await broker.schedule(runId, { input: { crash: true } });

    const takeOne = async () => {
      const r = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 1,
        })
      );
      return r.Messages?.[0];
    };

    const msg1 = await takeOne();
    if (!msg1) throw new Error("no message");
    const { handle: handle1 } = await broker.normalize(msg1);

    // Simulate crash: start processing but don't ack; shorten visibility so it reappears soon
    await handle1.extendVisibility?.(fastVisibility * 1000);
    // Pretend the worker died here: do not call ack/fail; just stop.
    // Wait for visibility to expire
    await new Promise((r) => setTimeout(r, (fastVisibility + 2) * 1000));

    // Second worker should receive it now
    let handled = false;
    const deadline = Date.now() + 30000;
    while (!handled) {
      const msg2 = await takeOne();
      if (msg2) {
        try {
          const { job: job2, handle: handle2 } = await broker.normalize(msg2);
          if (job2.runId === runId) {
            await processorB.handle(job2, handle2, wf, {
              heartbeatIntervalMs: 100,
            });
            handled = true;
            break;
          }
        } catch {}
      }
      if (Date.now() > deadline) throw new Error("no redelivered message");
      await new Promise((r) => setTimeout(r, 500));
    }

    const s = await store.load(runId);
    if (s.state?.status !== "done") throw new Error("workflow not done");

    const bucket = env("HOBO_S3_BUCKET")!;
    const prefix = normalizePrefix(env("HOBO_S3_PREFIX"));
    await s3.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: `${prefix}${runId}` })
    );
  }, 120000);

  it("many workers, duplicate messages, only one side-effect runs", async () => {
    type UpstreamWorkClient = {
      initOnce: (runId: string) => Promise<void>;
    };

    let calls = 0;
    const seen = new Set<string>();
    const extra: UpstreamWorkClient = {
      async initOnce(runId) {
        if (seen.has(runId)) return;
        seen.add(runId);
        calls += 1;
      },
    };

    type State = BaseWorkflowState & { step: "start" | "done" };
    const wf: Workflow<State> = fromSteps<State>(
      async (runId) => ({ runId, status: "running", step: "start" }),
      {
        start: async (s) => {
          await effectOnce(s, `init:${s.runId}`, () => extra.initOnce(s.runId));
          s.step = "done";
          s.status = "done";
          return { state: s, next: done() };
        },
        done: async (s) => ({ state: s, next: done() }),
      }
    );

    const runId = `e2e-dup-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2)}` as RunId;
    const s3 = new S3Client({});
    const sqs = new SQSClient({});
    const store: S3StateStore<State> = makeS3StateStoreFromEnv<State>(s3);
    const broker: SqsBroker = makeSqsBrokerFromEnv(sqs);
    const queueUrl = env("HOBO_SQS_URL")!;

    await drainQueue(sqs, queueUrl);

    const processor = new Processor<State>({
      broker,
      store,
    });

    // Enqueue duplicates (unique dedupe keys to avoid FIFO dedupe)
    for (let i = 0; i < 8; i++) {
      await broker.schedule(runId, {
        input: { i },
        dedupeKey: `${runId}-${i}-${Date.now()}`,
      });
    }

    // Start many workers; each pulls and handles at most one matching message per iteration
    const workerCount = 8;
    const until = Date.now() + 60000;
    const pollOne = async () => {
      const r = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 1,
        })
      );
      const m = r.Messages?.[0];
      if (!m) return 0;
      try {
        const { job, handle } = await broker.normalize(m);
        if (job.runId !== runId) return 0;
        await processor.handle(job, handle, wf, { heartbeatIntervalMs: 100 });
        return 1;
      } catch {
        return 0;
      }
    };
    for (;;) {
      await Promise.all(Array.from({ length: workerCount }, () => pollOne()));

      const s = await store.load(runId);
      if (s.state?.status === "done") break;
      if (Date.now() > until) throw new Error("timeout waiting for done");
    }

    if (calls !== 1) throw new Error(`expected 1 init call, got ${calls}`);

    const bucket = env("HOBO_S3_BUCKET")!;
    const prefix = normalizePrefix(env("HOBO_S3_PREFIX"));
    await s3.send(
      new DeleteObjectCommand({ Bucket: bucket, Key: `${prefix}${runId}` })
    );
  }, 120000);
});

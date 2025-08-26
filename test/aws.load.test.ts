import { expect, test } from "vitest";
import z from "zod";
import {
  DeciderRegistry,
  defineWorkflow,
  registerActivity,
  S3BlobStore,
  SQSWorkQueue,
  WFStatus,
  WorkflowEngine,
  WorkflowRunner,
} from "../src";

const bucket = z.string().parse(process.env.HOBO_S3_BUCKET);
const queueUrl = z.string().parse(process.env.HOBO_SQS_URL);
const prefix = z.string().default("wf/").parse(process.env.HOBO_S3_PREFIX);

const RUN = process.env.HOBO_RUN_LOAD_TEST === "1";
const TOTAL = Number.parseInt(process.env.HOBO_LOAD_WORKFLOWS ?? "200", 10);
const WORKERS = Number.parseInt(process.env.HOBO_LOAD_WORKERS ?? "32", 10);
const SQS_WAIT = Number.parseInt(process.env.HOBO_SQS_WAIT ?? "10", 10);
const ACT_MS = Number.parseInt(process.env.HOBO_LOAD_ACTIVITY_MS ?? "0", 10);
const TIMEOUT_MS = Number.parseInt(
  process.env.HOBO_LOAD_TIMEOUT_MS ?? "180000",
  10
);

const tfn = RUN ? test : test.skip;

tfn(
  "load: aws s3+sqs throughput and latency",
  async () => {
    const ac = new AbortController();
    const signal = ac.signal;
    const actTimes: number[] = [];

    registerActivity("nop", async (_input: any) => {
      if (ACT_MS > 0) await new Promise((r) => setTimeout(r, ACT_MS));
      actTimes.push(Date.now());
      return { ok: true };
    });

    const wf = defineWorkflow("load:nop", function* (io, _params) {
      yield io.exec("nop", {});
      yield io.exec("nop", {});
      yield io.exec("nop", {});
      yield io.exec("nop", {});
      yield io.exec("nop", {});
      return yield io.complete({ ok: true });
    });

    const reg = new DeciderRegistry();
    reg.register("load:nop", wf);

    const storeWriter = new S3BlobStore({ bucket, prefix });
    const storeReader = new S3BlobStore({ bucket, prefix });
    const engineWriter = new WorkflowEngine(storeWriter, reg);
    const engineReader = new WorkflowEngine(storeReader, reg);
    const queue = new SQSWorkQueue<{ wfId: string; taskId: string }>({
      queueUrl,
    });
    const scheduler = new WorkflowRunner(engineWriter, storeWriter, queue, {
      executeLocally: false,
    });
    const workerRunner = new WorkflowRunner(engineReader, storeReader, queue);

    const wfIds: string[] = [];
    const startAt = new Map<string, number>();
    const endAt = new Map<string, number>();
    const completed = new Set<string>();

    const startedAtAll = Date.now();

    for (let i = 0; i < TOTAL; i++)
      wfIds.push(
        `ld-${Date.now()}-${i}-${Math.random().toString(36).slice(2, 8)}`
      );
    const wfSet = new Set(wfIds);

    await Promise.all(
      wfIds.map(async (wfId) => {
        startAt.set(wfId, Date.now());
        await engineWriter.create(signal, wfId, "load:nop", {});
        await engineWriter.tick(signal, wfId);
      })
    );

    const workerLoop = async () => {
      for (;;) {
        if (completed.size >= TOTAL) break;
        const msgs = await queue.receive(signal, 10, SQS_WAIT);
        if (!msgs.length) {
          if (completed.size >= TOTAL) break;
          continue;
        }
        for (const m of msgs) {
          const { wfId, taskId } = m.body as { wfId: string; taskId: string };
          if (!wfSet.has(wfId)) continue;
          const status: WFStatus = await workerRunner.processWorkMessage(
            signal,
            wfId,
            taskId
          );
          await queue.delete(signal, m.id);
          await scheduler.drainExecs(signal, wfId, "scheduler");
          if (status !== "running" && !completed.has(wfId)) {
            completed.add(wfId);
            endAt.set(wfId, Date.now());
          }
        }
      }
    };

    const workers = Array.from({ length: Math.max(1, WORKERS) }, () =>
      workerLoop()
    );

    await Promise.all(
      wfIds.map((wfId) => scheduler.drainExecs(signal, wfId, "scheduler"))
    );

    const deadline = Date.now() + TIMEOUT_MS;
    while (completed.size < TOTAL && Date.now() < deadline)
      await new Promise((r) => setTimeout(r, 50));
    ac.abort();
    await Promise.allSettled(workers);

    expect(completed.size).toBe(TOTAL);

    const finishedAtAll = Date.now();
    const totalMs = finishedAtAll - startedAtAll;
    const lats = wfIds
      .map((id) => endAt.get(id)! - startAt.get(id)!)
      .filter((n) => Number.isFinite(n));
    lats.sort((a, b) => a - b);
    const pct = (p: number) => {
      if (!lats.length) return 0;
      const rank = Math.ceil((p / 100) * lats.length);
      return lats[Math.min(lats.length - 1, Math.max(0, rank - 1))];
    };
    const sum = lats.reduce((a, b) => a + b, 0);
    const tps = (TOTAL * 1000) / Math.max(1, totalMs);
    const actTotal = actTimes.length;
    const actAvg = (actTotal * 1000) / Math.max(1, totalMs);
    const buckets = new Map<number, number>();
    for (const t of actTimes) {
      const s = Math.floor((t - startedAtAll) / 1000);
      buckets.set(s, (buckets.get(s) ?? 0) + 1);
    }
    const secs = Math.ceil(totalMs / 1000);
    const series: number[] = [];
    for (let s = 0; s <= secs; s++) series.push(buckets.get(s) ?? 0);
    const actPeak = series.reduce((m, n) => (n > m ? n : m), 0);

    console.log("load: total_wf", TOTAL);
    console.log("load: workers", WORKERS);
    console.log("load: sqs_wait", SQS_WAIT);
    console.log("load: activity_ms", ACT_MS);
    console.log("load: duration_ms", totalMs);
    console.log("load: throughput_wf_per_s", tps.toFixed(2));
    console.log("load: activity_total", actTotal);
    console.log("load: activity_throughput_avg_per_s", actAvg.toFixed(2));
    console.log("load: activity_peak_per_s", actPeak);
    console.log("load: activity_per_s_series", series.join(","));
    console.log(
      "load: latency_ms min/avg/p50/p90/p99/max",
      lats[0] ?? 0,
      Math.round(sum / Math.max(1, lats.length)),
      pct(50),
      pct(90),
      pct(99),
      lats[lats.length - 1] ?? 0
    );

    for (let i = 0; i < 10; i++) {
      const ac2 = new AbortController();
      const msgs = await queue.receive(ac2.signal, 10, 0);
      let anyOwn = false;
      for (const m of msgs) {
        const { wfId } = m.body as { wfId?: string };
        if (wfId && wfSet.has(wfId)) {
          await queue.delete(ac2.signal, m.id);
          anyOwn = true;
        }
      }
      if (!anyOwn) break;
    }
  },
  600000
);

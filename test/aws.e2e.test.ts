import { expect, test } from "vitest";
import z from "zod";
import {
  DeciderRegistry,
  defineWorkflow,
  registerActivity,
  S3BlobStore,
  S3EventStore,
  SQSWorkQueue,
  WFEvent,
  WorkflowEngine,
  WorkflowRunner,
} from "../src";

const bucket = z.string().parse(process.env.HOBO_S3_BUCKET);
const queueUrl = z.string().parse(process.env.HOBO_SQS_URL);
const prefix = z.string().default("wf/").parse(process.env.HOBO_S3_PREFIX);

const RUN_E2E = process.env.HOBO_RUN_E2E === "1";
const T = RUN_E2E ? test : test.skip;

T(
  "e2e: s3 blob + s3 events + sqs queue",
  async () => {
    const ac = new AbortController();
    const signal = ac.signal;

    registerActivity("greet", async (input: any) => {
      console.log("greet", input);
      return { message: `hi ${input?.name ?? "hobo"}` };
    });

    const hello = defineWorkflow("hello", function* (io, params) {
      const r = yield io.exec("greet", { name: params?.name ?? "hobo" });
      yield io.set("greeting", r);
      return yield io.complete(r);
    });

    const store1 = new S3BlobStore({ bucket, prefix });
    const events1 = new S3EventStore<WFEvent>({ bucket, prefix });

    const store2 = new S3BlobStore({ bucket, prefix });
    const events2 = new S3EventStore<WFEvent>({ bucket, prefix });

    const reg = new DeciderRegistry();
    reg.register("demo:hello", hello);

    const engine1 = new WorkflowEngine(store1, reg, undefined, events1);
    const engine2 = new WorkflowEngine(store2, reg, undefined, events2);
    const queue = new SQSWorkQueue<{ wfId: string; taskId: string }>({
      queueUrl,
    });
    const runner = new WorkflowRunner(engine1, store1, queue);
    const truth = new S3BlobStore({ bucket, prefix });

    const wfId = `e2e-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    await engine1.create(signal, wfId, "demo:hello", {
      params: { name: "world" },
    });
    await engine1.tick(signal, wfId);
    const scheduler = new WorkflowRunner(engine1, store1, queue, {
      executeLocally: false,
    });
    await scheduler.drainExecs(signal, wfId, "scheduler");

    // Workers: poll SQS and process messages fully through the runner
    const workerLoop = async (r: WorkflowRunner) => {
      for (let i = 0; i < 10; i++) {
        const msgs = await queue.receive(signal, 10, 0);
        let processed = false;
        for (const m of msgs) {
          const { wfId: id, taskId } = m.body as {
            wfId: string;
            taskId: string;
          };
          if (id !== wfId) continue;
          await r.processWorkMessage(signal, id, taskId);
          await queue.delete(signal, m.id);
          processed = true;
        }
        const got = await truth.get(signal, wfId);
        if ((got?.state?.status as string) !== "running") break;
        if (!processed) continue;
      }
    };
    await Promise.all([
      workerLoop(new WorkflowRunner(engine1, store1, queue)),
      workerLoop(new WorkflowRunner(engine2, store2, queue)),
    ]);
    const fin = await truth.get(signal, wfId);
    expect(fin?.state?.status).toBe("completed");

    const got = await store1.get(signal, wfId);
    expect(got?.state?.ctx?.greeting?.message).toBe("hi world");

    // Ensure queue drained for this wfId
    const msgs = await queue.receive(signal, 10, 1);
    for (const m of msgs) {
      if ((m.body as { wfId?: string })?.wfId === wfId)
        await queue.delete(signal, m.id);
    }
  },
  60000
);

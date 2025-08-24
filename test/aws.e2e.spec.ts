import { S3Client } from "@aws-sdk/client-s3";
import { SQSClient } from "@aws-sdk/client-sqs";
import { describe, expect, it } from "vitest";
import { registerActivity } from "../src/activities";
import { S3BlobStore } from "../src/backends/s3";
import { SQSWorkQueue } from "../src/backends/sqs";
import { DeciderRegistry, WorkflowEngine } from "../src/engine";
import { WorkflowRunner } from "../src/runner";
import { defineWorkflow } from "../src/wfkit";
import { loadAwsTestConfig } from "./aws.helpers";

const rand = () => Math.random().toString(36).slice(2);

registerActivity("fast", (input: any) => ({ ok: true, v: input?.v ?? 1 }));

describe("aws e2e", () => {
  it("s3: workflow completes using S3BlobStore", async () => {
    const { bucket, prefix, region } = loadAwsTestConfig();
    const store = new S3BlobStore({
      bucket,
      prefix,
      client: new S3Client({ region }),
    });
    const reg = new DeciderRegistry();
    reg.register(
      "demo:one_exec",
      defineWorkflow("one_exec", function* (io) {
        yield io.exec("fast");
        return yield io.complete();
      })
    );
    const engine = new WorkflowEngine(store, reg);
    const runner = new WorkflowRunner(engine, store);
    const wfId = `wf_s3_${rand()}`;
    await engine.create(wfId, "demo:one_exec", {} as any);
    const status = await runner.runToCompletion(wfId);
    expect(status).toBe("completed");
    const got = await store.get(wfId);
    expect(got?.state.status).toBe("completed");
  }, 60000);

  it("sqs: runner enqueues reserved task messages", async () => {
    const { bucket, prefix, queueUrl, region } = loadAwsTestConfig();
    const store = new S3BlobStore({
      bucket,
      prefix,
      client: new S3Client({ region }),
    });
    const reg = new DeciderRegistry();
    reg.register(
      "demo:one_exec2",
      defineWorkflow("one_exec2", function* (io) {
        yield io.exec("fast");
        return yield io.complete();
      })
    );
    const engine = new WorkflowEngine(store, reg);
    const queue = new SQSWorkQueue({
      queueUrl,
      client: new SQSClient({ region }),
    });
    const runner = new WorkflowRunner(engine, store, queue);
    const wfId = `wf_sqs_${rand()}`;
    await engine.create(wfId, "demo:one_exec2", {} as any);
    await engine.tick(wfId);
    await runner.drainExecs(wfId, "e2e", 10, 30);

    let found = false;
    for (let i = 0; i < 6 && !found; i++) {
      const msgs = await queue.receive(10, 10);
      for (const m of msgs) {
        const b: any = m.body;
        if (b && b.wfId === wfId) {
          found = true;
          await queue.delete(m.id);
          break;
        }
        await queue.delete(m.id);
      }
      if (!found) await new Promise((r) => setTimeout(r, 1000));
    }
    expect(found).toBe(true);
  }, 60000);
});

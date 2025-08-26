import { runActivity } from "./activities";
import type { BlobStore, ExecTask, WFStatus } from "./engine";
import { WorkflowEngine } from "./engine";
import { HoboError } from "./errors";
import type { WorkQueue } from "./queue";

export class WorkflowRunner {
  constructor(
    private engine: WorkflowEngine,
    private store: BlobStore,
    private queue?: WorkQueue,
    private opts?: {
      beforeEnqueue?: () => Promise<void> | void;
      executeLocally?: boolean;
    }
  ) {}

  async drainExecs(
    signal: AbortSignal,
    wfId: string,
    workerId: string,
    maxPerPass = 20,
    leaseSecs = 120
  ): Promise<WFStatus> {
    let lastStatus: WFStatus = "running";
    for (;;) {
      const leased = await this.engine.reserveReadyActivities(
        signal,
        wfId,
        workerId,
        maxPerPass,
        leaseSecs
      );
      if (this.queue && leased.length) {
        if (this.opts?.beforeEnqueue) await this.opts.beforeEnqueue();
        for (const t of leased) {
          await this.queue.send(signal, { wfId, taskId: t.id });
        }
        if (this.opts?.executeLocally === false) return lastStatus;
      }
      if (!leased.length) return lastStatus;
      const got = await this.store.get(signal, wfId);
      const ctx = got?.state?.ctx ?? {};
      const results: Array<{
        taskId: string;
        success: boolean;
        resultOrError: unknown;
        leaseToken?: number;
      }> = [];
      for (const t of leased) {
        let ok = false;
        let res: unknown;
        try {
          res = await runActivity(t as ExecTask, ctx);
          ok = true;
        } catch (e) {
          if (e instanceof HoboError) res = e.toJSON();
          else
            res = {
              type: "non_retryable",
              message: String((e as { message?: string })?.message ?? e),
            };
        }
        results.push({
          taskId: t.id,
          success: ok,
          resultOrError: res,
          leaseToken: t.lease?.token,
        });
      }
      const r = await this.engine.completeActivities(signal, wfId, results, {
        decideNow: true,
      });
      lastStatus = r.status;
      if (r.status === "running" && r.next_wake) {
        const t = new Date(r.next_wake);
        const got = await this.engine.tick(signal, wfId, t);
        lastStatus = got.status;
      }
    }
  }

  async runToCompletion(signal: AbortSignal, wfId: string): Promise<WFStatus> {
    for (;;) {
      const got = await this.engine.tick(signal, wfId);
      if (got.status !== "running") return got.status;
      if (got.next_wake) {
        const t = new Date(got.next_wake);
        if (t.getTime() > Date.now()) await this.engine.tick(signal, wfId, t);
      }
      const status = await this.drainExecs(signal, wfId, "local");
      if (status !== "running") return status;
    }
  }

  async processWorkMessage(
    signal: AbortSignal,
    wfId: string,
    taskId: string
  ): Promise<WFStatus> {
    const got = await this.store.get(signal, wfId);
    if (!got) throw new Error(`workflow not found: ${wfId}`);
    const s = got.state;
    const task = s.tasks[taskId] as ExecTask | undefined;
    if (!task || task.type !== "exec") return s.status;
    const ctx = s.ctx ?? {};
    let ok = false;
    let res: unknown;
    try {
      res = await runActivity(task, ctx);
      ok = true;
    } catch (e) {
      if (e instanceof HoboError) res = e.toJSON();
      else
        res = {
          type: "non_retryable",
          message: String((e as { message?: string })?.message ?? e),
        };
    }
    await this.engine.completeActivity(
      signal,
      wfId,
      taskId,
      ok,
      res,
      task.lease?.token
    );
    const t = await this.engine.tick(signal, wfId);
    return t.status;
  }
}

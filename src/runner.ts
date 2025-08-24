import { runActivity } from "./activities";
import type { BlobStore, ExecTask, WFStatus } from "./engine";
import { WorkflowEngine } from "./engine";
import { HoboError } from "./errors";
import type { WorkQueue } from "./queue";

export class WorkflowRunner {
  constructor(
    private engine: WorkflowEngine,
    private store: BlobStore,
    private queue?: WorkQueue
  ) {}

  async drainExecs(
    signal: AbortSignal,
    wfId: string,
    workerId: string,
    maxPerPass = 20,
    leaseSecs = 120
  ) {
    for (;;) {
      const leased = await this.engine.reserveReadyActivities(
        signal,
        wfId,
        workerId,
        maxPerPass,
        leaseSecs
      );
      if (this.queue && leased.length) {
        for (const t of leased) {
          await this.queue.send(signal, { wfId, taskId: t.id });
        }
      }
      if (!leased.length) return;
      for (const t of leased) {
        let ok = false;
        let res: unknown;
        try {
          const got = await this.store.get(signal, wfId);
          const ctx = got?.state?.ctx ?? {};
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
        await this.engine.completeActivity(
          signal,
          wfId,
          t.id,
          ok,
          res,
          t.lease?.token
        );
        await this.engine.tick(signal, wfId);
      }
    }
  }

  async runToCompletion(signal: AbortSignal, wfId: string): Promise<WFStatus> {
    for (;;) {
      await this.drainExecs(signal, wfId, "local");
      const got = await this.engine.tick(signal, wfId);
      if (got.status !== "running") return got.status;
      if (got.next_wake) {
        const t = new Date(got.next_wake);
        await this.engine.tick(signal, wfId, t);
      }
    }
  }
}

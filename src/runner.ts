import { runTask } from "./actions";
import type { BlobStore, ExecTask, WFStatus } from "./engine";
import { WorkflowEngine } from "./engine";

export class WorkflowRunner {
  constructor(private engine: WorkflowEngine, private store: BlobStore) {}

  async drainExecs(
    wfId: string,
    workerId: string,
    maxPerPass = 20,
    leaseSecs = 120
  ) {
    for (;;) {
      const leased = await this.engine.reserveReadyActivities(
        wfId,
        workerId,
        maxPerPass,
        leaseSecs
      );
      if (!leased.length) return;
      for (const t of leased) {
        let ok = false;
        let res: any;
        try {
          const got = await this.store.get(wfId);
          const ctx = got?.state?.ctx ?? {};
          res = await runTask(t as ExecTask, ctx);
          ok = true;
        } catch (e: any) {
          res = { message: String(e?.message ?? e) };
        }
        await this.engine.completeActivity(wfId, t.id, ok, res, t.lease?.token);
        await this.engine.tick(wfId);
      }
    }
  }

  async runToCompletion(wfId: string): Promise<WFStatus> {
    for (;;) {
      await this.drainExecs(wfId, "local");
      const got = await this.engine.tick(wfId);
      if (got.status !== "running") return got.status;
      if (got.next_wake) {
        const t = new Date(got.next_wake);
        await this.engine.tick(wfId, t);
      }
    }
  }
}

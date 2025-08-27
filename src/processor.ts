import {
  BaseWorkflowState,
  BrokerJob,
  EngineDeps,
  EngineOptions,
  EpochMillis,
  JobHandle,
  TickContext,
  VersionTag,
  Workflow,
  WorkflowProcessor,
} from "./types";

export class ConflictError extends Error {
  constructor(message?: string) {
    super(message ?? "conflict");
    this.name = "ConflictError";
  }
}

export class Processor<S extends BaseWorkflowState = BaseWorkflowState>
  implements WorkflowProcessor<S>
{
  constructor(private readonly deps: EngineDeps<S>) {}

  async handle(
    job: BrokerJob,
    handle: JobHandle,
    wf: Workflow<S>,
    opts?: EngineOptions
  ): Promise<void> {
    const { clock, store, events } = this.deps;
    events?.emit({ type: "handle.start", runId: job.runId });
    const now = () => (clock ? clock() : (Date.now() as EpochMillis));

    const ctx: TickContext = { now };

    const loaded = await store.load(job.runId);
    if (store.shouldProcess) {
      const ok = await store.shouldProcess(job.runId, loaded.version);
      if (!ok) {
        await handle.ack();
        return;
      }
    }
    let state: S;
    let version: VersionTag | null = loaded.version;
    if (!loaded.state) {
      events?.emit({ type: "state.init", runId: job.runId });
      if (!wf.init) throw new Error("Missing workflow init for new run");
      state = await wf.init(job.runId, job.input, ctx);
      try {
        version = await store.createIfAbsent(job.runId, state);
      } catch (err) {
        if (err instanceof ConflictError) {
          events?.emit({ type: "init.conflict", runId: job.runId });
          const re = await store.load(job.runId);
          if (!re.state || !re.version) throw err;
          state = re.state as S;
          version = re.version;
        } else {
          throw err;
        }
      }
    } else {
      events?.emit({ type: "state.load", runId: job.runId });
      state = loaded.state;
    }

    for (let i = 0; i < 1000; i++) {
      const { state: nextState, next } = await wf.tick(state, ctx);
      if (!version) throw new Error("Missing version token");
      try {
        const newVersion = await store.save(job.runId, version, nextState);
        state = nextState;
        version = newVersion;
      } catch (err) {
        if (err instanceof ConflictError) {
          events?.emit({ type: "save.conflict", runId: job.runId });
          const re = await store.load(job.runId);
          if (!re.state || !re.version) throw err;
          state = re.state as S;
          version = re.version;
          continue;
        }
        throw err;
      }

      if (next.type === "continue") continue;
      if (next.type === "waitFor") {
        events?.emit({ type: "wait.for", runId: job.runId, ms: next.ms });
        await handle.requeue(next.ms);
        return;
      }
      if (next.type === "waitUntil") {
        const delay = Math.max(0, next.at - now());
        events?.emit({
          type: "wait.until",
          runId: job.runId,
          at: next.at,
          delay,
        });
        await handle.requeue(delay);
        return;
      }
      if (next.type === "done") {
        events?.emit({ type: "done", runId: job.runId });
        await handle.ack();
        return;
      }
      if (next.type === "fail") {
        events?.emit({
          type: "fail",
          runId: job.runId,
          reason: next.reason ?? "failed",
        });
        await handle.fail(next.reason ?? "failed");
        return;
      }
    }
    void opts;
    events?.emit({ type: "loop.limit", runId: job.runId, iterations: 1000 });
    throw new Error("Processor loop limit reached");
  }
}

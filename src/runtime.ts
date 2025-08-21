// runtime.ts
import { runTask } from "./actions";
import {
  DeciderRegistry,
  ExecTask,
  InMemoryBlobStore,
  WorkflowEngine,
} from "./engine";
import { aggregate, approval, hello, retry, race, all_mixed, until, signals_multi } from "./workflows";

// Build the engine once at process start (swap store for S3/Azure/GCS CAS impl)
export const store = new InMemoryBlobStore();
export const registry = new DeciderRegistry();
registry.register("demo:hello", hello);
registry.register("demo:approval", approval);
registry.register("demo:aggregate", aggregate);
registry.register("demo:retry", retry);
registry.register("demo:race", race);
registry.register("demo:all_mixed", all_mixed);
registry.register("demo:until", until);
registry.register("demo:signals_multi", signals_multi);

export const engine = new WorkflowEngine(store, registry);

/** Drain up to N ready exec tasks for a workflow. Call this from your "work" queue worker. */
export async function drainExecsForWorkflow(
  wfId: string,
  workerId: string,
  maxPerPass = 20,
  leaseSecs = 120
) {
  for (;;) {
    const leased = await engine.reserveReadyActivities(
      wfId,
      workerId,
      maxPerPass,
      leaseSecs
    );
    if (!leased.length) return;

    for (const t of leased) {
      let success = false;
      let result: any;
      try {
        // Provide current ctx to action; fetch fresh each loop to reflect prior updates
        const got = await store.get(wfId);
        const ctx = got?.state?.ctx ?? {};
        result = await runTask(t as ExecTask, ctx);
        success = true;
      } catch (err: any) {
        result = { message: String(err?.message ?? err) };
      }
      await engine.completeActivity(
        wfId,
        t.id,
        success,
        result,
        t.lease?.token
      );
      await engine.tick(wfId); // let the decider react immediately (schedule sleeps/next steps)
    }
    // loop to catch more immediately-ready tasks
  }
}

/** Example "kick" function (without a real queue): creates and ticks a workflow once. */
export async function startHelloDemo() {
  const wfId = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(wfId, "demo:hello", { params: { start: 0 } });
  await engine.tick(wfId); // schedules first exec
  await drainExecsForWorkflow(wfId, "local"); // simulate a work nudge
  // For timers, you’d normally schedule a delayed tick at state.next_wake via your queue/scheduler.
  return wfId;
}

async function runToCompletion(wfId: string) {
  for (;;) {
    await drainExecsForWorkflow(wfId, "local");
    const got = await engine.tick(wfId);
    if (got.status !== "running") return got.status;
    if (got.next_wake) {
      const t = new Date(got.next_wake);
      await engine.tick(wfId, t);
    }
  }
}

async function demoAll() {
  const ids: Record<string, string> = {};
  ids.hello = await startHelloDemo();
  ids.retry = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(ids.retry, "demo:retry", { params: {} });
  await runToCompletion(ids.retry);

  ids.race = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(ids.race, "demo:race", { params: {} });
  await runToCompletion(ids.race);

  ids.all_mixed = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(ids.all_mixed, "demo:all_mixed", { params: {} });
  await runToCompletion(ids.all_mixed);

  ids.until = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(ids.until, "demo:until", { params: {} });
  await runToCompletion(ids.until);

  ids.aggregate = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(ids.aggregate, "demo:aggregate", { params: { id: 42 } });
  await runToCompletion(ids.aggregate);

  ids.signals_multi = "wf_" + Math.random().toString(36).slice(2, 8);
  await engine.create(ids.signals_multi, "demo:signals_multi", { params: {} });
  await engine.signal(ids.signals_multi, "A", { x: 1 });
  await engine.signal(ids.signals_multi, "B", { y: 2 });
  await runToCompletion(ids.signals_multi);

  return ids;
}

/** Queue integration sketch (BullMQ/Graphile/SQS):
 * - On tick job: const { next_wake, status } = await engine.tick(wfId);
 *   if (status === 'running') { scheduleNextTick(next_wake); enqueueWorkNudge(wfId); }
 * - On work job: await drainExecsForWorkflow(wfId, workerId);
 *   const got = await engine.tick(wfId); if (got.status === 'running') scheduleNextTick(got.next_wake);
 */

async function main() {
  const ids = await demoAll();
  console.log("wfIds", ids);
}

main();

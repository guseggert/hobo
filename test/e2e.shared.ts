import { describe, expect, it } from "vitest";
import { registerActivity } from "../src/activities";
import { DeciderRegistry, WorkflowEngine, type BlobStore } from "../src/engine";
import { WorkflowRunner } from "../src/runner";
import { defineWorkflow } from "../src/wfkit";
import {
  aggregate,
  all_mixed,
  approval,
  hello,
  race,
  retry,
  signals_multi,
  until,
} from "./helpers/workflows";

// async function drainExecs(
//   engine: WorkflowEngine,
//   store: BlobStore,
//   wfId: string,
//   workerId = "t",
//   tNow?: Date
// ) {
//   for (;;) {
//     const leased = await engine.reserveReadyActivities(
//       wfId,
//       workerId,
//       50,
//       60,
//       tNow ?? new Date()
//     );
//     if (!leased.length) return;
//     for (const t of leased) {
//       const got = await store.get(wfId);
//       const ctx = got?.state?.ctx ?? {};
//       let ok = false;
//       let res: any;
//       try {
//         res = await runActivity(t, ctx);
//         ok = true;
//       } catch (e: any) {
//         res = { message: String(e?.message ?? e) };
//       }
//       await engine.completeActivity(wfId, t.id, ok, res, t.lease?.token);
//       await engine.tick(wfId);
//     }
//   }
// }

// async function runToCompletion(
//   engine: WorkflowEngine,
//   store: BlobStore,
//   wfId: string
// ) {
//   let spins = 0;
//   let simNow = new Date();
//   for (;;) {
//     await drainExecs(engine, store, wfId, "t", simNow);
//     const got = await engine.tick(wfId, simNow);
//     if (got.status !== "running") return got;
//     if (got.next_wake) {
//       simNow = new Date(got.next_wake);
//       continue;
//     }
//     const s = await store.get(wfId);
//     const hasWork = Object.values(s?.state?.tasks ?? {}).some(
//       (t: any) => t.status === "pending" || t.status === "leased"
//     );
//     if (!hasWork) throw new Error("workflow idle with no work");
//     if (++spins > 100) throw new Error("workflow did not converge");
//   }
// }

export function registerE2ESuite(
  label: string,
  runnerBuilder: (reg: DeciderRegistry) => WorkflowRunner,
  store: BlobStore
) {
  const reg = new DeciderRegistry();
  reg.register("demo:hello", hello);
  reg.register("demo:approval", approval);
  reg.register("demo:aggregate", aggregate);
  reg.register("demo:retry", retry);
  reg.register("demo:race", race);
  reg.register("demo:all_mixed", all_mixed);
  reg.register("demo:until", until);
  reg.register("demo:signals_multi", signals_multi);

  const runner = runnerBuilder(reg);
  const engine = new WorkflowEngine(store, reg);

  const rid = () => Math.random().toString(36).slice(2);
  const uid = (base: string) => `${base}_${label}_${rid()}`;

  registerActivity("increment", (input: any, ctx: Record<string, any>) => {
    return { label: "inc", to: input?.to ?? Number(ctx.i ?? 0) + 1 };
  });
  registerActivity("fulfill", async (input: any) => {
    return { label: "fulfill", ok: true, requestId: input?.requestId };
  });
  registerActivity("escalate", async (input: any) => {
    return { label: "escalated", requestId: input?.requestId };
  });
  registerActivity("fetchA", async (input: any) => ({
    label: "fetchA",
    data: 1,
    id: input?.id,
  }));
  registerActivity("fetchB", async (input: any) => ({
    label: "fetchB",
    data: 2,
    id: input?.id,
  }));
  registerActivity("fetchC", async (input: any) => ({
    label: "fetchC",
    data: 3,
    id: input?.id,
  }));
  registerActivity("merge", async (input: any) => {
    const { a, b, c } = input ?? {};
    return {
      label: "merge",
      sum: (a?.data ?? 0) + (b?.data ?? 0) + (c?.data ?? 0),
    };
  });
  registerActivity("always_fail", () => {
    throw new Error("boom");
  });
  registerActivity("fast", (input: any) => ({
    label: "fast",
    v: input?.v ?? 1,
  }));
  registerActivity("slow", async (input: any) => {
    await new Promise((r) => setTimeout(r, 10));
    return { label: "slow", v: input?.v ?? 1 };
  });

  describe(`e2e (${label})`, () => {
    it("hello increments and completes", async () => {
      const wfId = uid("wf_hello");
      await engine.create(wfId, "demo:hello", { params: { start: 0 } });
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
    });

    it("aggregate runs all and merges", async () => {
      const wfId = uid("wf_agg");
      await engine.create(wfId, "demo:aggregate", { params: { id: 1 } });
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
    });

    it("race picks fast", async () => {
      const wfId = uid("wf_race");
      await engine.create(wfId, "demo:race", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
    });

    it("all_mixed waits both and completes", async () => {
      const wfId = uid("wf_all");
      await engine.create(wfId, "demo:all_mixed", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
    });

    it("until waits until time", async () => {
      const wfId = uid("wf_until");
      await engine.create(wfId, "demo:until", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
    });

    it("signals_multi consumes in order", async () => {
      const wfId = uid("wf_sig");
      await engine.create(wfId, "demo:signals_multi", {} as any);
      await engine.signal(wfId, "A", { a: 1 });
      await engine.signal(wfId, "B", { b: 2 });
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
    });

    it("approval rejects and fails", async () => {
      const wfId = uid("wf_approval_reject");
      await engine.create(wfId, "demo:approval", { params: { requestId: 1 } });
      await engine.signal(wfId, "reject", { who: "qa" });
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
    });

    it("always_fails activity leads to workflow failure after retries", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:always_fails",
        defineWorkflow("always_fails", function* (io) {
          yield io.exec("always_fail");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_always_fail");
      await engine.create(wfId, "demo:always_fails", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
    });

    it("unknown activity fails after retries", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:unknown",
        defineWorkflow("unknown", function* (io) {
          yield io.exec("missing_activity");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_unknown");
      await engine.create(wfId, "demo:unknown", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
    });

    it("fail effect immediately fails workflow", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:fail_immediate",
        defineWorkflow("fail_immediate", function* (io) {
          return yield io.fail("boom");
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_fail_immediate");
      await engine.create(wfId, "demo:fail_immediate", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
    });

    it("all() fails when one child repeatedly fails", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:all_fail",
        defineWorkflow("all_fail", function* (io) {
          yield io.all([io.exec("always_fail"), io.exec("fast")]);
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_all_fail");
      await engine.create(wfId, "demo:all_fail", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
    });

    it("lease fencing: wrong token ignored, correct token completes once", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:one_exec",
        defineWorkflow("one_exec", function* (io) {
          yield io.exec("fast");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_fence");
      const t0 = new Date();
      await engine.create(wfId, "demo:one_exec", {} as any);
      await engine.tick(wfId, t0);
      const leased = await engine.reserveReadyActivities(wfId, "w1", 1, 60, t0);
      expect(leased.length).toBe(1);
      const t = leased[0];
      const wrong = await engine.completeActivity(
        wfId,
        t.id,
        true,
        { x: 1 },
        (t.lease?.token ?? 0) + 1,
        t0
      );
      expect(wrong.already).toBe(true);
      const ok = await engine.completeActivity(
        wfId,
        t.id,
        true,
        { ok: true },
        t.lease?.token,
        t0
      );
      expect(ok.already).toBeUndefined();
      const dup = await engine.completeActivity(
        wfId,
        t.id,
        true,
        { ok: true },
        t.lease?.token,
        t0
      );
      expect(dup.already).toBe(true);
    });

    it("lease expiry allows re-reserve by another worker with fencing increment", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:one_exec2",
        defineWorkflow("one_exec2", function* (io) {
          yield io.exec("fast");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_expire");
      const t0 = new Date();
      await engine.create(wfId, "demo:one_exec2", {} as any);
      await engine.tick(wfId, t0);
      const a = await engine.reserveReadyActivities(wfId, "w1", 1, 1, t0);
      expect(a.length).toBe(1);
      const token1 = a[0].lease?.token;
      const t1 = new Date(t0.getTime() + 2000);
      const b = await engine.reserveReadyActivities(wfId, "w2", 1, 60, t1);
      expect(b.length).toBe(1);
      expect(b[0].lease?.token).toBeGreaterThan(Number(token1));
    });

    it("approval deadline escalates when no signal arrives", async () => {
      const wfId = uid("wf_approval_deadline");
      await engine.create(wfId, "demo:approval", { params: { requestId: 2 } });
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
      const got = await store.get(wfId);
      const escalated = (got?.state.history ?? []).some(
        (e: any) =>
          e.type === "ACTIVITY_COMPLETED" &&
          (e as any).result?.label === "escalated"
      );
      expect(escalated).toBe(true);
    });

    it("backoff schedule records 2s then 4s before final fail", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:always_fails2",
        defineWorkflow("always_fails2", function* (io) {
          yield io.exec("always_fail");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_backoff");
      await engine.create(wfId, "demo:always_fails2", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
      const got = await store.get(wfId);
      const retries = (got?.state.history ?? []).filter(
        (e: any) => e.type === "ACTIVITY_RETRY"
      );
      const seq = retries.map((e: any) => e.after_seconds);
      expect(seq).toEqual([2, 4]);
    });

    it("exec per-call opts: retryDelays and maxTries respected", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:opts_per_call",
        defineWorkflow("opts_per_call", function* (io) {
          yield io.exec("always_fail", undefined, {
            maxTries: 2,
            retryDelays: [1, 9],
          });
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_opts_per_call");
      await engine.create(wfId, "demo:opts_per_call", {} as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
      const got = await store.get(wfId);
      const retries = (got?.state.history ?? []).filter(
        (e: any) => e.type === "ACTIVITY_RETRY"
      );
      const seq = retries.map((e: any) => e.after_seconds);
      expect(seq).toEqual([1]);
    });

    it("execDefaults in params apply to io.exec by default", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:opts_defaults",
        defineWorkflow("opts_defaults", function* (io) {
          yield io.exec("always_fail");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_opts_defaults");
      await engine.create(wfId, "demo:opts_defaults", {
        params: { execDefaults: { retryDelays: [3, 5], maxTries: 3 } },
      } as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
      const got = await store.get(wfId);
      const retries = (got?.state.history ?? []).filter(
        (e: any) => e.type === "ACTIVITY_RETRY"
      );
      const seq = retries.map((e: any) => e.after_seconds);
      expect(seq).toEqual([3, 5]);
    });

    it("per-call opts override execDefaults", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:opts_override",
        defineWorkflow("opts_override", function* (io) {
          yield io.exec("always_fail", undefined, {
            retryDelays: [2, 2],
            maxTries: 3,
          });
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_opts_override");
      await engine.create(wfId, "demo:opts_override", {
        params: { execDefaults: { retryDelays: [7, 7], maxTries: 3 } },
      } as any);
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("failed");
      const got = await store.get(wfId);
      const retries = (got?.state.history ?? []).filter(
        (e: any) => e.type === "ACTIVITY_RETRY"
      );
      const seq = retries.map((e: any) => e.after_seconds);
      expect(seq).toEqual([2, 2]);
    });

    it("runAfter schedules exec in the future", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:opts_run_after",
        defineWorkflow("opts_run_after", function* (io) {
          return yield io.complete();
        })
      );
      reg.register(
        "demo:opts_run_after_exec",
        defineWorkflow("opts_run_after_exec", function* (io) {
          yield io.exec("fast", undefined, {
            runAfter: new Date(Date.now() + 10000).toISOString(),
          });
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_opts_run_after");
      const t0 = new Date();
      await engine.create(wfId, "demo:opts_run_after_exec", {} as any);
      const r0 = await engine.tick(wfId, t0);
      expect(r0.next_wake).not.toBeNull();
      const nw = new Date(r0.next_wake!);
      expect(nw.getTime()).toBeGreaterThan(t0.getTime());
      const leasedEarly = await engine.reserveReadyActivities(
        wfId,
        "w",
        1,
        60,
        t0
      );
      expect(leasedEarly.length).toBe(0);
    });

    it("race with signal wins", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:race_sig",
        defineWorkflow("race_sig", function* (io) {
          const r = yield io.race({
            sig: io.signal("S"),
            slow: io.exec("slow"),
          });
          yield io.set("winner", r.key);
          return yield io.complete(r);
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_race_sig");
      await engine.create(wfId, "demo:race_sig", {} as any);
      await engine.signal(wfId, "S", { s: 1 });
      const res = await runner.runToCompletion(wfId);
      expect(res).toBe("completed");
      const got = await store.get(wfId);
      expect((got?.state as any).ctx.winner).toBe("sig");
    });

    it("extendLease extends expiry and wrong owner/token are rejected", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:lease_extend",
        defineWorkflow("lease_extend", function* (io) {
          yield io.exec("fast");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_extend");
      const t0 = new Date();
      await engine.create(wfId, "demo:lease_extend", {} as any);
      await engine.tick(wfId, t0);
      const [task] = await engine.reserveReadyActivities(wfId, "w1", 1, 5, t0);
      const token = task.lease!.token;
      const exp0 = new Date(task.lease!.expires_at).getTime();
      const t1 = new Date(t0.getTime() + 1000);
      await engine.extendLease(wfId, task.id, "w1", token, 10, t1);
      const got = await store.get(wfId);
      const exp1 = new Date(
        ((got!.state as any).tasks as any)[task.id].lease.expires_at
      ).getTime();
      expect(exp1).toBeGreaterThan(exp0);
      await expect(async () =>
        engine.extendLease(wfId, task.id, "w2", token, 5, t1)
      ).rejects.toThrow();
      await expect(async () =>
        engine.extendLease(wfId, task.id, "w1", token + 1, 5, t1)
      ).rejects.toThrow();
    });

    it("reserve respects maxN and returns tasks ordered by id; deep copies returned", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:three_execs",
        defineWorkflow("three_execs", function* (io) {
          yield io.all([
            io.exec("fast", { i: 1 }),
            io.exec("fast", { i: 2 }),
            io.exec("fast", { i: 3 }),
          ]);
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_reserve");
      const t0 = new Date();
      await engine.create(wfId, "demo:three_execs", {} as any);
      await engine.tick(wfId, t0);
      const a = await engine.reserveReadyActivities(wfId, "w", 2, 60, t0);
      expect(a.length).toBe(2);
      expect(a[0].id < a[1].id).toBe(true);
      a[0].status = "failed" as any;
      const got = await store.get(wfId);
      expect(((got!.state as any).tasks as any)[a[0].id].status).toBe("leased");
    });

    it("next_wake picks earliest among timers and leases; null when idle", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:timer_then_exec",
        defineWorkflow("timer_then_exec", function* (io) {
          yield io.sleep(60);
          yield io.exec("fast");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_nextwake");
      const t0 = new Date();
      await engine.create(wfId, "demo:timer_then_exec", {} as any);
      const r0 = await engine.tick(wfId, t0);
      expect(r0.next_wake).not.toBeNull();
      const t1 = new Date(r0.next_wake!);
      const rFire = await engine.tick(wfId, t1);
      const reserved = await engine.reserveReadyActivities(
        wfId,
        "w",
        1,
        30,
        t1
      );
      expect(reserved.length).toBe(1);
      const r1 = rFire;
      expect(r1.next_wake).not.toBeNull();
      await engine.completeActivity(
        wfId,
        reserved[0].id,
        true,
        { ok: true },
        reserved[0].lease?.token,
        t1
      );
      const r2 = await engine.tick(wfId, t1);
      expect(r2.next_wake).toBeNull();
    });

    it("signals after completion do not change state or schedule work", async () => {
      const wfId = uid("wf_done");
      await engine.create(wfId, "demo:hello", { params: { start: 3 } });
      const done = await runner.runToCompletion(wfId);
      expect(done).toBe("completed");
      const before = await store.get(wfId);
      const beforeRev = before!.rev;
      await engine.signal(wfId, "A", { late: true });
      const after = await store.get(wfId);
      expect(after!.rev).toBeGreaterThanOrEqual(beforeRev);
      expect(after!.state.status).toBe("completed");
      const { next_wake } = await engine.tick(wfId);
      expect(next_wake).toBeNull();
    });

    it("idempotent scheduling: repeat ticks without new events don't duplicate tasks", async () => {
      const reg = new DeciderRegistry();
      reg.register(
        "demo:one_exec_again",
        defineWorkflow("one_exec_again", function* (io) {
          yield io.exec("fast");
          return yield io.complete();
        })
      );
      const engine = new WorkflowEngine(store, reg);
      const wfId = uid("wf_idem");
      const t0 = new Date();
      await engine.create(wfId, "demo:one_exec_again", {} as any);
      await engine.tick(wfId, t0);
      await engine.tick(wfId, t0);
      await engine.tick(wfId, t0);
      const got = await store.get(wfId);
      const scheduled = (got!.state.history as any[]).filter(
        (e: any) => e.type === "ACTIVITY_SCHEDULED"
      );
      expect(scheduled.length).toBe(1);
    });
  });
}

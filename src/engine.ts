import { HoboError, HoboErrorObject, HoboErrorObjectType } from "./errors";
import type { EventStore, TaskStore } from "./model";

export type WFStatus = "running" | "completed" | "failed" | "cancelled";
export type TaskStatus = "pending" | "leased" | "completed" | "failed";

export interface EventBase {
  ts: string;
  type: string;
}
export type WFEvent =
  | (EventBase & { type: "WF_CREATED" })
  | (EventBase & { type: "WF_COMPLETED" })
  | (EventBase & { type: "WF_FAILED"; reason?: HoboErrorObjectType })
  | (EventBase & {
      type: "TIMER_SCHEDULED";
      task_id: string;
      run_after: string;
      label?: string;
    })
  | (EventBase & { type: "TIMER_FIRED"; task_id: string; label?: string })
  | (EventBase & { type: "ACTIVITY_SCHEDULED"; task_id: string; name: string })
  | (EventBase & { type: "ACTIVITY_COMPLETED"; task_id: string; result?: any })
  | (EventBase & {
      type: "ACTIVITY_FAILED";
      task_id: string;
      error: HoboErrorObjectType;
    })
  | (EventBase & {
      type: "ACTIVITY_RETRY";
      task_id: string;
      after_seconds: number;
    })
  | (EventBase & { type: "CTX_SET"; key: string })
  | (EventBase & { type: "SIGNAL"; name: string; payload?: any });

export interface Lease {
  owner: string;
  expires_at: string;
  token: number;
}

export interface BaseTask {
  id: string;
  type: "sleep" | "exec";
  status: TaskStatus;
  run_after: string;
  result?: any;
  error?: HoboErrorObjectType;
}

export interface SleepTask extends BaseTask {
  type: "sleep";
  label?: string; // for DSL correlation
}

export interface ExecTask extends BaseTask {
  type: "exec";
  name?: string;
  code: string; // opaque payload for your workers (we'll use named activities)
  lease?: Lease | null;
  tries?: number;
  max_tries?: number;
  idem_key?: string;
  fence?: number; // monotonic counter for lease tokens
  retry_delays?: number[];
  // fields to support external task stores
  kind?: "activity";
  createdAt?: string;
  updatedAt?: string;
  version?: string;
}

export type Task = SleepTask | ExecTask;

export interface WFState {
  id: string;
  rev: number;
  status: WFStatus;
  created_at: string;
  updated_at: string;

  ctx: Record<string, any>;
  history: WFEvent[];
  tasks: Record<string, Task>;

  need_decide: boolean;
  next_wake: string | null;
  seq: number;

  decider: string;
  signals?: Array<{ ts: string; name: string; payload: any }>;
  last_seq?: number;
}

export type Command =
  | { type: "sleep"; seconds?: number; until?: string; label?: string }
  | {
      type: "exec";
      name?: string;
      code: string;
      run_after?: string;
      idem_key?: string;
      max_tries?: number;
      retry_delays?: number[];
    }
  | { type: "set"; key: string; value: any }
  | { type: "complete_workflow" }
  | { type: "fail_workflow"; reason?: unknown };

export type Decider = (
  ctx: Record<string, any>,
  history: WFEvent[]
) => Command[];

// Conflict errors are modeled via HoboError with type: "conflict".

const now = () => new Date();
const iso = (d: Date) => d.toISOString();
const parseISO = (s: string) => new Date(s);
const addSeconds = (d: Date, s: number) => new Date(d.getTime() + s * 1000);

export interface BlobStore {
  get(
    signal: AbortSignal,
    key: string
  ): Promise<{ rev: number; state: WFState; cas?: string } | null>;
  put(
    signal: AbortSignal,
    key: string,
    state: WFState,
    cas?: string | null
  ): Promise<number>; // returns new rev
}

export class InMemoryBlobStore implements BlobStore {
  private db = new Map<string, { rev: number; blob: string; cas: string }>();
  async get(_signal: AbortSignal, key: string) {
    const rec = this.db.get(key);
    if (!rec) return null;
    return {
      rev: rec.rev,
      state: JSON.parse(rec.blob) as WFState,
      cas: rec.cas,
    };
  }
  async put(
    _signal: AbortSignal,
    key: string,
    state: WFState,
    cas: string | null = null
  ) {
    const rec = this.db.get(key);
    if (!rec) {
      if (cas !== null) throw HoboError.conflict("No record");
      const rev = 1;
      const newRec = { rev, blob: JSON.stringify(state), cas: `r${rev}` };
      this.db.set(key, newRec);
      return rev;
    }
    if (cas === null || cas !== rec.cas)
      throw HoboError.conflict("CAS mismatch");
    const rev = rec.rev + 1;
    rec.rev = rev;
    rec.cas = `r${rev}`;
    rec.blob = JSON.stringify(state);
    return rev;
  }
}

// ---- Decider registry ----
export class DeciderRegistry {
  private map = new Map<string, Decider>();
  register(name: string, fn: Decider) {
    this.map.set(name, fn);
  }
  get(name: string): Decider {
    const fn = this.map.get(name);
    if (!fn) throw new Error(`Unknown decider: ${name}`);
    return fn;
  }
}

// ---- Engine ----
export class WorkflowEngine {
  constructor(
    private store: BlobStore,
    private deciders: DeciderRegistry,
    private taskStore?: TaskStore<any>,
    private eventStore?: EventStore<WFEvent>
  ) {}

  async create(
    signal: AbortSignal,
    wfId: string,
    deciderName: string,
    initialCtx: Record<string, any> = {}
  ) {
    const t = now();
    const s: WFState = {
      id: wfId,
      rev: 0,
      status: "running",
      created_at: iso(t),
      updated_at: iso(t),
      ctx: { ...initialCtx },
      history: [],
      tasks: {},
      need_decide: true,
      next_wake: null,
      seq: 0,
      decider: deciderName,
      last_seq: 0,
    };
    await this.store.put(signal, wfId, s, null);
    await this.logEvent(signal, s, { ts: iso(t), type: "WF_CREATED" });
    const got = await this.store.get(signal, wfId);
    await this.store.put(signal, wfId, s, got?.cas ?? null);
    return s;
  }

  async tick(signal: AbortSignal, wfId: string, tNow: Date = now()) {
    // CAS retry loop
    for (;;) {
      const got = await this.store.get(signal, wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { state: s, cas } = got;

      // 1) Fire due timers
      for (const task of Object.values(s.tasks)) {
        if (task.type === "sleep" && task.status === "pending") {
          if (parseISO(task.run_after) <= tNow) {
            task.status = "completed";
            await this.logEvent(signal, s, {
              ts: iso(tNow),
              type: "TIMER_FIRED",
              task_id: task.id,
              label: task.label,
            });
            s.need_decide = true;
          }
        }
      }

      // 2) Decider
      if (s.status === "running" && s.need_decide) {
        const cmds = this.deciders.get(s.decider)(s.ctx, s.history);
        await this.applyCommands(signal, s, cmds, tNow);
        s.need_decide = false;
      }

      // 3) next_wake
      s.next_wake = this.computeNextWake(s);
      s.updated_at = iso(tNow);

      try {
        const newRev = await this.store.put(signal, wfId, s, cas ?? null);
        return {
          rev: newRev,
          next_wake: s.next_wake,
          status: s.status,
        };
      } catch (e) {
        if (e instanceof HoboError && e.obj.type === "conflict") continue;
        throw e;
      }
    }
  }

  async reserveReadyActivities(
    signal: AbortSignal,
    wfId: string,
    workerId: string,
    maxN = 1,
    leaseSecs = 120,
    tNow: Date = now()
  ): Promise<ExecTask[]> {
    for (;;) {
      const got = await this.store.get(signal, wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { state: s, cas } = got;

      const leased: ExecTask[] = [];
      const tasks = Object.values(s.tasks).sort((a, b) =>
        a.id.localeCompare(b.id)
      );
      for (const tsk of tasks) {
        if (tsk.type !== "exec") continue;
        if (tsk.status === "completed" || tsk.status === "failed") continue;

        // if leased, check expiry
        if (tsk.status === "leased") {
          const lease = tsk.lease;
          if (lease && parseISO(lease.expires_at) > tNow) continue; // still leased
        }

        // pending and due?
        if (parseISO(tsk.run_after) <= tNow) {
          tsk.status = "leased";
          const nextToken = (tsk.fence ?? 0) + 1;
          tsk.fence = nextToken;
          tsk.lease = {
            owner: workerId,
            token: nextToken,
            expires_at: iso(addSeconds(tNow, leaseSecs)),
          };
          leased.push(tsk);
          if (leased.length >= maxN) break;
        }
      }

      if (!leased.length) return [];

      try {
        await this.store.put(signal, wfId, s, cas ?? null);
        return JSON.parse(JSON.stringify(leased)) as ExecTask[]; // deep copy
      } catch (e) {
        if (e instanceof HoboError && e.obj.type === "conflict") continue;
        throw e;
      }
    }
  }

  /**
   * Complete (or fail) an activity. If `leaseToken` is provided, only the current lessee may complete.
   * Returns { already: true } if task is already terminal (idempotent).
   */
  async completeActivity(
    signal: AbortSignal,
    wfId: string,
    taskId: string,
    success: boolean,
    resultOrError: any,
    leaseToken?: number,
    tNow: Date = now()
  ): Promise<{ rev: number; status: WFStatus; already?: true }> {
    for (;;) {
      const got = await this.store.get(signal, wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { state: s, cas } = got;

      const t = s.tasks[taskId] as ExecTask | undefined;
      if (!t) return { rev: got.rev, status: s.status, already: true };
      if (t.status === "completed" || t.status === "failed")
        return { rev: got.rev, status: s.status, already: true };

      // Fencing guard
      if (
        t.status !== "leased" ||
        !t.lease ||
        (leaseToken !== undefined && t.lease.token !== leaseToken)
      ) {
        // Stale or not yours; ignore safely
        return { rev: got.rev, status: s.status, already: true };
      }

      if (success) {
        t.status = "completed";
        t.result = resultOrError;
        await this.logEvent(signal, s, {
          ts: iso(tNow),
          type: "ACTIVITY_COMPLETED",
          task_id: t.id,
          result: t.result,
        });
      } else {
        t.tries = (t.tries ?? 0) + 1;
        let err: HoboErrorObjectType;
        if (resultOrError instanceof HoboError) err = resultOrError.toJSON();
        else {
          try {
            err = HoboErrorObject.parse(resultOrError);
          } catch {
            err = {
              type: "non_retryable",
              message: String(
                (resultOrError as { message?: string })?.message ??
                  resultOrError
              ),
              cause: undefined,
            };
          }
        }
        t.error = err;
        if (t.tries >= (t.max_tries ?? 3)) {
          t.status = "failed";
          await this.logEvent(signal, s, {
            ts: iso(tNow),
            type: "ACTIVITY_FAILED",
            task_id: t.id,
            error: err,
          });
          s.status = "failed";
        } else {
          const del = t.retry_delays?.[t.tries - 1];
          const backoff = del ?? Math.min(300, 2 ** t.tries);
          t.status = "pending";
          t.lease = null;
          t.run_after = iso(addSeconds(tNow, backoff));
          await this.logEvent(signal, s, {
            ts: iso(tNow),
            type: "ACTIVITY_RETRY",
            task_id: t.id,
            after_seconds: backoff,
          });
        }
      }
      s.need_decide = true;
      s.updated_at = iso(tNow);

      try {
        const newRev = await this.store.put(signal, wfId, s, cas ?? null);
        return { rev: newRev, status: s.status };
      } catch (e) {
        if (e instanceof HoboError && e.obj.type === "conflict") continue;
        throw e;
      }
    }
  }

  /** Extend an active lease (heartbeat). Throws if not leased by `owner` with `token`. */
  async extendLease(
    signal: AbortSignal,
    wfId: string,
    taskId: string,
    owner: string,
    token: number,
    extraSeconds: number,
    tNow: Date = now()
  ) {
    for (;;) {
      const got = await this.store.get(signal, wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { state: s, cas } = got;
      const t = s.tasks[taskId] as ExecTask | undefined;
      if (!t || t.type !== "exec" || t.status !== "leased" || !t.lease)
        throw new Error("not leased");
      if (t.lease.owner !== owner || t.lease.token !== token)
        throw new Error("not your lease");
      const currentExp = parseISO(t.lease.expires_at);
      if (currentExp < tNow) throw new Error("lease expired");
      t.lease.expires_at = iso(addSeconds(currentExp, extraSeconds));
      try {
        await this.store.put(signal, wfId, s, cas ?? null);
        return;
      } catch (e) {
        if (e instanceof HoboError && e.obj.type === "conflict") continue;
        throw e;
      }
    }
  }

  async signal(
    signal: AbortSignal,
    wfId: string,
    name: string,
    payload: any,
    tNow: Date = now()
  ) {
    for (;;) {
      const got = await this.store.get(signal, wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { state: s, cas } = got;
      s.signals ??= [];
      s.signals.push({ ts: iso(tNow), name, payload });
      await this.logEvent(signal, s, {
        ts: iso(tNow),
        type: "SIGNAL",
        name,
        payload,
      });
      s.need_decide = true;
      s.updated_at = iso(tNow);
      try {
        const newRev = await this.store.put(signal, wfId, s, cas ?? null);
        return { rev: newRev };
      } catch (e) {
        if (e instanceof HoboError && e.obj.type === "conflict") continue;
        throw e;
      }
    }
  }

  // ---- helpers ----
  private async applyCommands(
    signal: AbortSignal,
    s: WFState,
    cmds: Command[],
    tNow: Date
  ) {
    for (const c of cmds) {
      switch (c.type) {
        case "sleep":
          await this.scheduleSleep(
            signal,
            s,
            tNow,
            c.seconds,
            c.until,
            c.label
          );
          break;
        case "exec":
          await this.scheduleExec(
            signal,
            s,
            tNow,
            c.name,
            c.code,
            c.run_after,
            c.idem_key,
            c.max_tries,
            c.retry_delays
          );
          break;
        case "set":
          this.setPath(s.ctx, c.key, c.value);
          await this.logEvent(signal, s, {
            ts: iso(tNow),
            type: "CTX_SET",
            key: c.key,
          });
          break;
        case "complete_workflow":
          s.status = "completed";
          await this.logEvent(signal, s, {
            ts: iso(tNow),
            type: "WF_COMPLETED",
          });
          break;
        case "fail_workflow": {
          s.status = "failed";
          let err: HoboErrorObjectType;
          const input = c.reason;
          if (input instanceof HoboError) err = input.toJSON();
          else {
            try {
              err = HoboErrorObject.parse(input);
            } catch {
              err = {
                type: "non_retryable",
                message:
                  input !== undefined
                    ? String((input as { message?: string })?.message ?? input)
                    : "workflow failed",
                cause: undefined,
              };
            }
          }
          await this.logEvent(signal, s, {
            ts: iso(tNow),
            type: "WF_FAILED",
            reason: err,
          });
          break;
        }
      }
    }
  }

  private nextTaskId(s: WFState): string {
    s.seq = (s.seq ?? 0) + 1;
    return `t${String(s.seq).padStart(6, "0")}`;
  }

  private async scheduleSleep(
    signal: AbortSignal,
    s: WFState,
    tNow: Date,
    seconds?: number,
    until?: string,
    label?: string
  ) {
    if ((seconds == null) === (until == null))
      throw new Error("sleep requires exactly one of seconds or until");
    const runAfter = until
      ? parseISO(until)
      : addSeconds(tNow, Number(seconds));
    const id = this.nextTaskId(s);
    const task: SleepTask = {
      id,
      type: "sleep",
      status: "pending",
      run_after: iso(runAfter),
      label,
    };
    s.tasks[id] = task;
    await this.logEvent(signal, s, {
      ts: iso(tNow),
      type: "TIMER_SCHEDULED",
      task_id: id,
      run_after: iso(runAfter),
      label,
    });
  }

  private async scheduleExec(
    signal: AbortSignal,
    s: WFState,
    tNow: Date,
    name: string | undefined,
    code: string,
    run_after?: string,
    idem_key?: string,
    max_tries?: number,
    retry_delays?: number[]
  ) {
    const id = this.nextTaskId(s);
    const task: ExecTask = {
      id,
      type: "exec",
      name: name ?? "exec",
      code,
      status: "pending",
      run_after: run_after ?? iso(tNow),
      lease: null,
      tries: 0,
      max_tries: max_tries ?? 3,
      idem_key,
      fence: 0,
    };
    if (retry_delays && Array.isArray(retry_delays))
      task.retry_delays = retry_delays.slice();
    // populate external-store friendly fields
    task.kind = "activity";
    task.createdAt = iso(tNow);
    task.updatedAt = iso(tNow);
    task.version = "0";
    if (this.taskStore) {
      await this.taskStore.put(signal, s.id, id, task, undefined);
    }
    await this.logEvent(signal, s, {
      ts: iso(tNow),
      type: "ACTIVITY_SCHEDULED",
      task_id: id,
      name: task.name ?? "exec",
    });
  }

  private computeNextWake(s: WFState): string | null {
    let next: Date | null = null;
    for (const t of Object.values(s.tasks)) {
      if (t.status === "pending") {
        const due = parseISO(t.run_after);
        if (!next || due < next) next = due;
      } else if (t.status === "leased" && (t as ExecTask).lease) {
        const exp = parseISO((t as ExecTask).lease!.expires_at);
        if (!next || exp < next) next = exp;
      }
    }
    return next ? iso(next) : null;
  }

  /** dot-path setter (e.g., "results.total" => ctx.results.total = value) */
  private setPath(obj: any, path: string, value: any) {
    const parts = path.split(".");
    let p = obj;
    while (parts.length > 1) {
      const k = parts.shift()!;
      p[k] ??= {};
      p = p[k];
    }
    p[parts[0]] = value;
  }
  private async logEvent(signal: AbortSignal, s: WFState, ev: WFEvent) {
    s.history.push(ev);
    if (this.eventStore) {
      const next = (s.last_seq ?? 0) + 1;
      await this.eventStore.put(signal, s.id, next, ev);
      s.last_seq = next;
    }
  }
}

// engine.ts
// Single-blob workflow engine with CAS, durable timers, activity leases (with fencing tokens).

export type WFStatus = "running" | "completed" | "failed" | "cancelled";
export type TaskStatus = "pending" | "leased" | "completed" | "failed";

export interface EventBase {
  ts: string;
  type: string;
}
export type WFEvent =
  | (EventBase & { type: "WF_CREATED" })
  | (EventBase & { type: "WF_COMPLETED" })
  | (EventBase & { type: "WF_FAILED"; reason?: string })
  | (EventBase & {
      type: "TIMER_SCHEDULED";
      task_id: string;
      run_after: string;
      label?: string;
    })
  | (EventBase & { type: "TIMER_FIRED"; task_id: string; label?: string })
  | (EventBase & { type: "ACTIVITY_SCHEDULED"; task_id: string; name: string })
  | (EventBase & { type: "ACTIVITY_COMPLETED"; task_id: string; result?: any })
  | (EventBase & { type: "ACTIVITY_FAILED"; task_id: string; error: any })
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
  error?: any;
}

export interface SleepTask extends BaseTask {
  type: "sleep";
  label?: string; // for DSL correlation
}

export interface ExecTask extends BaseTask {
  type: "exec";
  name?: string;
  code: string; // opaque payload for your workers (we'll use named actions)
  lease?: Lease | null;
  tries?: number;
  max_tries?: number;
  idem_key?: string;
  fence?: number; // monotonic counter for lease tokens
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
}

export type Command =
  | { type: "sleep"; seconds?: number; until?: string; label?: string }
  | {
      type: "exec";
      name?: string;
      code: string;
      run_after?: string;
      idem_key?: string;
    }
  | { type: "set"; key: string; value: any }
  | { type: "complete_workflow" }
  | { type: "fail_workflow"; reason?: string };

export type Decider = (
  ctx: Record<string, any>,
  history: WFEvent[]
) => Command[];

export class ConflictError extends Error {
  constructor(msg = "Revision conflict") {
    super(msg);
  }
}

// ---- Utilities ----
const now = () => new Date();
const iso = (d: Date) => d.toISOString();
const parseISO = (s: string) => new Date(s);
const addSeconds = (d: Date, s: number) => new Date(d.getTime() + s * 1000);

// ---- Blob store interface (swap with S3/Azure/GCS impl for production) ----
export interface BlobStore {
  get(key: string): Promise<{ rev: number; state: WFState } | null>;
  put(
    key: string,
    state: WFState,
    expectedRev?: number | null
  ): Promise<number>; // returns new rev
}

export class InMemoryBlobStore implements BlobStore {
  private db = new Map<string, { rev: number; blob: string }>();
  async get(key: string) {
    const rec = this.db.get(key);
    if (!rec) return null;
    return { rev: rec.rev, state: JSON.parse(rec.blob) as WFState };
  }
  async put(key: string, state: WFState, expectedRev: number | null = null) {
    const rec = this.db.get(key);
    if (!rec) {
      if (expectedRev && expectedRev !== 0)
        throw new ConflictError("No record, unexpected expectedRev");
      const rev = 1;
      this.db.set(key, { rev, blob: JSON.stringify(state) });
      return rev;
    }
    if (expectedRev !== null && expectedRev !== rec.rev)
      throw new ConflictError();
    const rev = rec.rev + 1;
    rec.rev = rev;
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
  constructor(private store: BlobStore, private deciders: DeciderRegistry) {}

  async create(
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
      history: [{ ts: iso(t), type: "WF_CREATED" }],
      tasks: {},
      need_decide: true,
      next_wake: null,
      seq: 0,
      decider: deciderName,
    };
    await this.store.put(wfId, s, null);
    return s;
  }

  async tick(wfId: string, tNow: Date = now()) {
    // CAS retry loop
    for (;;) {
      const got = await this.store.get(wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { rev, state: s } = got;

      // 1) Fire due timers
      for (const t of Object.values(s.tasks)) {
        if (t.type === "sleep" && t.status === "pending") {
          if (parseISO(t.run_after) <= tNow) {
            t.status = "completed";
            s.history.push({
              ts: iso(tNow),
              type: "TIMER_FIRED",
              task_id: t.id,
              label: (t as SleepTask).label,
            });
            s.need_decide = true;
          }
        }
      }

      // 2) Decider
      if (s.status === "running" && s.need_decide) {
        const cmds = this.deciders.get(s.decider)(s.ctx, s.history);
        this.applyCommands(s, cmds, tNow);
        s.need_decide = false;
      }

      // 3) next_wake
      s.next_wake = this.computeNextWake(s);
      s.updated_at = iso(tNow);

      try {
        const newRev = await this.store.put(wfId, s, rev);
        return {
          rev: newRev,
          next_wake: s.next_wake,
          status: s.status as WFStatus,
        };
      } catch (e) {
        if (e instanceof ConflictError) continue;
        throw e;
      }
    }
  }

  async reserveReadyActivities(
    wfId: string,
    workerId: string,
    maxN = 1,
    leaseSecs = 120,
    tNow: Date = now()
  ): Promise<ExecTask[]> {
    for (;;) {
      const got = await this.store.get(wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { rev, state: s } = got;

      const leased: ExecTask[] = [];
      const tasks = Object.values(s.tasks).sort((a, b) =>
        a.id.localeCompare(b.id)
      );
      for (const t of tasks) {
        if (t.type !== "exec") continue;
        if (t.status === "completed" || t.status === "failed") continue;

        // if leased, check expiry
        if (t.status === "leased") {
          if (t.lease && parseISO(t.lease.expires_at) > tNow) continue; // still leased
        }

        // pending and due?
        if (parseISO(t.run_after) <= tNow) {
          t.status = "leased";
          const nextToken = (t.fence ?? 0) + 1;
          t.fence = nextToken;
          (t as ExecTask).lease = {
            owner: workerId,
            token: nextToken,
            expires_at: iso(addSeconds(tNow, leaseSecs)),
          };
          leased.push(t as ExecTask);
          if (leased.length >= maxN) break;
        }
      }

      if (!leased.length) return [];

      try {
        await this.store.put(wfId, s, rev);
        return JSON.parse(JSON.stringify(leased)) as ExecTask[]; // deep copy
      } catch (e) {
        if (e instanceof ConflictError) continue;
        throw e;
      }
    }
  }

  /**
   * Complete (or fail) an activity. If `leaseToken` is provided, only the current lessee may complete.
   * Returns { already: true } if task is already terminal (idempotent).
   */
  async completeActivity(
    wfId: string,
    taskId: string,
    success: boolean,
    resultOrError: any,
    leaseToken?: number,
    tNow: Date = now()
  ): Promise<{ rev: number; status: WFStatus; already?: true }> {
    for (;;) {
      const got = await this.store.get(wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { rev, state: s } = got;

      const t = s.tasks[taskId] as ExecTask | undefined;
      if (!t) return { rev, status: s.status as WFStatus, already: true };
      if (t.status === "completed" || t.status === "failed")
        return { rev, status: s.status as WFStatus, already: true };

      // Fencing guard
      if (
        t.status !== "leased" ||
        !t.lease ||
        (leaseToken !== undefined && t.lease.token !== leaseToken)
      ) {
        // Stale or not yours; ignore safely
        return { rev, status: s.status as WFStatus, already: true };
      }

      if (success) {
        t.status = "completed";
        t.result = resultOrError;
        s.history.push({
          ts: iso(tNow),
          type: "ACTIVITY_COMPLETED",
          task_id: t.id,
          result: t.result,
        });
      } else {
        t.tries = (t.tries ?? 0) + 1;
        t.error = resultOrError;
        if (t.tries >= (t.max_tries ?? 3)) {
          t.status = "failed";
          s.history.push({
            ts: iso(tNow),
            type: "ACTIVITY_FAILED",
            task_id: t.id,
            error: t.error,
          });
          s.status = "failed";
        } else {
          const backoff = Math.min(300, 2 ** t.tries);
          t.status = "pending";
          t.lease = null;
          t.run_after = iso(addSeconds(tNow, backoff));
          s.history.push({
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
        const newRev = await this.store.put(wfId, s, rev);
        return { rev: newRev, status: s.status as WFStatus };
      } catch (e) {
        if (e instanceof ConflictError) continue;
        throw e;
      }
    }
  }

  /** Extend an active lease (heartbeat). Throws if not leased by `owner` with `token`. */
  async extendLease(
    wfId: string,
    taskId: string,
    owner: string,
    token: number,
    extraSeconds: number,
    tNow: Date = now()
  ) {
    for (;;) {
      const got = await this.store.get(wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { rev, state: s } = got;
      const t = s.tasks[taskId] as ExecTask | undefined;
      if (!t || t.type !== "exec" || t.status !== "leased" || !t.lease)
        throw new Error("not leased");
      if (t.lease.owner !== owner || t.lease.token !== token)
        throw new Error("not your lease");
      const currentExp = parseISO(t.lease.expires_at);
      if (currentExp < tNow) throw new Error("lease expired");
      t.lease.expires_at = iso(addSeconds(currentExp, extraSeconds));
      try {
        await this.store.put(wfId, s, rev);
        return;
      } catch (e) {
        if (e instanceof ConflictError) continue;
        throw e;
      }
    }
  }

  async signal(wfId: string, name: string, payload: any, tNow: Date = now()) {
    for (;;) {
      const got = await this.store.get(wfId);
      if (!got) throw new Error(`workflow not found: ${wfId}`);
      const { rev, state: s } = got;
      s.signals ??= [];
      s.signals.push({ ts: iso(tNow), name, payload });
      s.history.push({ ts: iso(tNow), type: "SIGNAL", name, payload });
      s.need_decide = true;
      s.updated_at = iso(tNow);
      try {
        const newRev = await this.store.put(wfId, s, rev);
        return { rev: newRev };
      } catch (e) {
        if (e instanceof ConflictError) continue;
        throw e;
      }
    }
  }

  // ---- helpers ----
  private applyCommands(s: WFState, cmds: Command[], tNow: Date) {
    for (const c of cmds) {
      switch (c.type) {
        case "sleep":
          this.scheduleSleep(s, tNow, c.seconds, c.until, c.label);
          break;
        case "exec":
          this.scheduleExec(s, tNow, c.name, c.code, c.run_after, c.idem_key);
          break;
        case "set":
          this.setPath(s.ctx, c.key, c.value);
          s.history.push({ ts: iso(tNow), type: "CTX_SET", key: c.key });
          break;
        case "complete_workflow":
          s.status = "completed";
          s.history.push({ ts: iso(tNow), type: "WF_COMPLETED" });
          break;
        case "fail_workflow":
          s.status = "failed";
          s.history.push({
            ts: iso(tNow),
            type: "WF_FAILED",
            reason: c.reason,
          });
          break;
      }
    }
  }

  private nextTaskId(s: WFState): string {
    s.seq = (s.seq ?? 0) + 1;
    return `t${String(s.seq).padStart(6, "0")}`;
  }

  private scheduleSleep(
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
    s.history.push({
      ts: iso(tNow),
      type: "TIMER_SCHEDULED",
      task_id: id,
      run_after: iso(runAfter),
      label,
    });
  }

  private scheduleExec(
    s: WFState,
    tNow: Date,
    name: string | undefined,
    code: string,
    run_after?: string,
    idem_key?: string
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
      max_tries: 3,
      idem_key,
      fence: 0,
    };
    s.tasks[id] = task;
    s.history.push({
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
}

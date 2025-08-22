import type { Command, Decider, WFEvent } from "./engine";

export type Effect<T = any> =
  | { kind: "exec"; name: string; input?: any; opts?: ExecOpts }
  | { kind: "sleep"; seconds?: number; until?: string }
  | { kind: "signal"; name: string }
  | { kind: "all"; children: Effect<any>[] }
  | { kind: "race"; options: Record<string, Effect<any>> }
  | { kind: "set"; key: string; value: any }
  | { kind: "complete"; value?: any }
  | { kind: "fail"; reason?: string };

export type ExecOpts = {
  maxTries?: number;
  retryDelays?: number[];
  idemKey?: string;
  runAfter?: string;
};

export type IO = {
  exec: <T = any>(name: string, input?: any, opts?: ExecOpts) => Effect<T>;
  sleep: (seconds: number) => Effect<void>;
  until: (iso: string) => Effect<void>;
  signal: <T = any>(name: string) => Effect<T>;
  all: <T extends any[]>(effects: {
    [K in keyof T]: Effect<T[K]>;
  }) => Effect<T>;
  race: <T extends Record<string, any>>(opts: {
    [K in keyof T]: Effect<T[K]>;
  }) => Effect<{ key: string; value: T[keyof T] }>;
  set: (key: string, value: any) => Effect<void>;
  complete: (value?: any) => Effect<never>;
  fail: (reason?: string) => Effect<never>;
};

export type WorkflowGen = (io: IO, params?: any) => Generator<Effect, any, any>;

export function defineWorkflow(_name: string, gen: WorkflowGen): Decider {
  return (ctx, history) => interpret(gen, ctx, history);
}

const ACT_PREFIX = "E:";
const TIMER_PREFIX = "S:";

function IOImpl(): IO {
  return {
    exec: (name, input, opts) => ({ kind: "exec", name, input, opts }),
    sleep: (seconds) => ({ kind: "sleep", seconds }),
    until: (iso) => ({ kind: "sleep", until: iso }),
    signal: (name) => ({ kind: "signal", name }),
    all: (children: Effect[]) => ({ kind: "all", children }),
    race: (options: Record<string, Effect>) => ({ kind: "race", options }),
    set: (key, value) => ({ kind: "set", key, value }),
    complete: (value?: any) => ({ kind: "complete", value }),
    fail: (reason?: string) => ({ kind: "fail", reason }),
  };
}

type HistIndex = {
  execScheduledById: Record<string, string>;
  execCompletedByTask: Record<string, any>;
  timerScheduledById: Record<string, string>;
  timerFiredByTask: Record<string, true>;
  signalsByName: Record<string, { ts: string; payload: any }[]>;
  raceOrder: string[];
};

function indexHistory(history: WFEvent[]): HistIndex {
  const execScheduledById: Record<string, string> = {};
  const execCompletedByTask: Record<string, any> = {};
  const timerScheduledById: Record<string, string> = {};
  const timerFiredByTask: Record<string, true> = {};
  const signalsByName: Record<string, { ts: string; payload: any }[]> = {};
  const raceOrder: string[] = [];

  for (const e of history) {
    if (e.type === "ACTIVITY_SCHEDULED") {
      const name = (e as any).name as string | undefined;
      if (name && name.startsWith(ACT_PREFIX)) {
        const effectId = name.substring(ACT_PREFIX.length);
        execScheduledById[effectId] = (e as any).task_id;
      }
    } else if (e.type === "ACTIVITY_COMPLETED") {
      execCompletedByTask[(e as any).task_id] = (e as any).result;
      raceOrder.push((e as any).task_id);
    } else if (e.type === "TIMER_SCHEDULED") {
      const label = (e as any).label as string | undefined;
      if (label && label.startsWith(TIMER_PREFIX)) {
        const effectId = label.substring(TIMER_PREFIX.length);
        timerScheduledById[effectId] = (e as any).task_id;
      }
    } else if (e.type === "TIMER_FIRED") {
      timerFiredByTask[(e as any).task_id] = true;
      raceOrder.push((e as any).task_id);
    } else if (e.type === "SIGNAL") {
      const name = (e as any).name as string;
      (signalsByName[name] ??= []).push({
        ts: (e as any).ts,
        payload: (e as any).payload,
      });
    }
  }
  return {
    execScheduledById,
    execCompletedByTask,
    timerScheduledById,
    timerFiredByTask,
    signalsByName,
    raceOrder,
  };
}

function interpret(
  gen: WorkflowGen,
  ctx: Record<string, any>,
  history: WFEvent[]
): Command[] {
  const io = IOImpl();
  const h = indexHistory(history);

  // deterministic cursor per tick; each yield gets a stable effect id
  let cursor = 0;
  const nextId = (suffix?: string) =>
    suffix ? `${cursor}${suffix}` : `${cursor}`;

  // signal consumption offsets
  const sigCount: Record<string, number> = (ctx.$wf?.sigCount ?? {}) as any;

  // Commands to emit
  const setCmds: Command[] = [];
  const commands: Command[] = [];

  // local mutation helper (also stage into setCmds)
  const setInternal = (key: string, value: any) => {
    setCmds.push({ type: "set", key, value });
    // mutate local mirror for subsequent steps in this same tick
    const parts = key.split(".");
    let p: any = ctx;
    while (parts.length > 1) {
      const k = parts.shift()!;
      p[k] ??= {};
      p = p[k];
    }
    p[parts[0]] = value;
  };

  // read status of an effect from history
  const slotOf = (id: string, eff: Effect) => {
    if (eff.kind === "exec") {
      const taskId = h.execScheduledById[id];
      if (!taskId) return { status: "pending" as const };
      const done = h.execCompletedByTask[taskId];
      return done !== undefined
        ? { status: "done" as const, value: done }
        : { status: "waiting" as const, taskId };
    } else if (eff.kind === "sleep") {
      const taskId = h.timerScheduledById[id];
      if (!taskId) return { status: "pending" as const };
      return h.timerFiredByTask[taskId]
        ? { status: "done" as const }
        : { status: "waiting" as const, taskId };
    }
    return { status: "na" as const };
  };

  // run the generator; feed results for ready steps; stop when we must wait
  const iter = gen(io, ctx.params);
  let input: any = undefined;

  // restore cursor at start of turn
  if (cursor === 0 && !ctx.$wf)
    setCmds.push({
      type: "set",
      key: "$wf",
      value: { cursor: 0, sigCount: {} },
    });

  for (;;) {
    const step = iter.next(input);
    if (step.done) {
      commands.push({ type: "complete_workflow" });
      break;
    }

    const eff = step.value as Effect;
    cursor += 1;
    setInternal("$wf.cursor", cursor);
    const eid = nextId();

    if (eff.kind === "exec") {
      const s = slotOf(eid, eff);
      if (s.status === "done") {
        input = s.value;
        continue;
      }
      if (s.status === "pending") {
        const code = JSON.stringify({
          action: eff.name,
          input: eff.input ?? null,
        });
        const defaults = (ctx as any)?.params?.execDefaults ?? {};
        const o = { ...defaults, ...(eff.opts ?? {}) } as ExecOpts;
        commands.push({
          type: "exec",
          name: `${ACT_PREFIX}${eid}`,
          code,
          run_after: o.runAfter,
          idem_key: o.idemKey,
          max_tries: o.maxTries,
          retry_delays: o.retryDelays,
        });
      }
      break; // wait
    } else if (eff.kind === "sleep") {
      const s = slotOf(eid, eff);
      if (s.status === "done") {
        input = undefined;
        continue;
      }
      if (s.status === "pending") {
        const cmd: Command = eff.until
          ? { type: "sleep", until: eff.until, label: `${TIMER_PREFIX}${eid}` }
          : {
              type: "sleep",
              seconds: eff.seconds ?? 0,
              label: `${TIMER_PREFIX}${eid}`,
            };
        commands.push(cmd);
      }
      break; // wait
    } else if (eff.kind === "signal") {
      const consumed = sigCount[eff.name] ?? 0;
      const list = h.signalsByName[eff.name] ?? [];
      if (list.length > consumed) {
        setInternal(`$wf.sigCount.${eff.name}`, consumed + 1);
        input = list[consumed].payload;
        continue;
      }
      break; // wait
    } else if (eff.kind === "set") {
      setInternal(eff.key, eff.value);
      input = undefined;
      continue;
    } else if (eff.kind === "complete") {
      if (eff.value !== undefined) setInternal("result", eff.value);
      commands.push({ type: "complete_workflow" });
      break;
    } else if (eff.kind === "fail") {
      commands.push({ type: "fail_workflow", reason: eff.reason });
      break;
    } else if (eff.kind === "all") {
      let allDone = true;
      const results: any[] = [];
      for (let i = 0; i < eff.children.length; i++) {
        const child = eff.children[i];
        const cid = `${eid}.${i}`;
        const cs = slotOf(cid, child);
        if (cs.status === "pending") {
          if (child.kind === "exec") {
            const defaults = (ctx as any)?.params?.execDefaults ?? {};
            const o = {
              ...defaults,
              ...((child as any).opts ?? {}),
            } as ExecOpts;
            commands.push({
              type: "exec",
              name: `${ACT_PREFIX}${cid}`,
              code: JSON.stringify({
                action: (child as any).name,
                input: (child as any).input ?? null,
              }),
              run_after: o.runAfter,
              idem_key: o.idemKey,
              max_tries: o.maxTries,
              retry_delays: o.retryDelays,
            });
          } else if (child.kind === "sleep") {
            commands.push(
              (child as any).until
                ? {
                    type: "sleep",
                    until: (child as any).until,
                    label: `${TIMER_PREFIX}${cid}`,
                  }
                : {
                    type: "sleep",
                    seconds: (child as any).seconds ?? 0,
                    label: `${TIMER_PREFIX}${cid}`,
                  }
            );
          }
          allDone = false;
        } else if (cs.status === "waiting") {
          allDone = false;
        } else if (cs.status === "done") {
          results[i] = child.kind === "exec" ? (cs as any).value : undefined;
        }
      }
      if (allDone) {
        input = results;
        continue;
      }
      break;
    } else if (eff.kind === "race") {
      const keyByTask: Record<string, string> = {};
      const keys = Object.keys(eff.options);
      let sigWinner: {
        key: string;
        name: string;
        value: any;
        ts: string;
      } | null = null;
      for (const key of keys) {
        const child = eff.options[key];
        const cid = `${eid}.${key}`;
        const cs = slotOf(cid, child);
        if (cs.status === "pending") {
          if (child.kind === "exec") {
            const defaults = (ctx as any)?.params?.execDefaults ?? {};
            const o = {
              ...defaults,
              ...((child as any).opts ?? {}),
            } as ExecOpts;
            commands.push({
              type: "exec",
              name: `${ACT_PREFIX}${cid}`,
              code: JSON.stringify({
                action: (child as any).name,
                input: (child as any).input ?? null,
              }),
              run_after: o.runAfter,
              idem_key: o.idemKey,
              max_tries: o.maxTries,
              retry_delays: o.retryDelays,
            });
          } else if (child.kind === "sleep") {
            commands.push(
              (child as any).until
                ? {
                    type: "sleep",
                    until: (child as any).until,
                    label: `${TIMER_PREFIX}${cid}`,
                  }
                : {
                    type: "sleep",
                    seconds: (child as any).seconds ?? 0,
                    label: `${TIMER_PREFIX}${cid}`,
                  }
            );
          }
        }
        let tid: string | undefined;
        if (child.kind === "exec") tid = h.execScheduledById[cid];
        else if (child.kind === "sleep") tid = h.timerScheduledById[cid];
        if (tid) keyByTask[tid] = key;
        if (child.kind === "signal") {
          const name = (child as any).name as string;
          const consumed = sigCount[name] ?? 0;
          const list = h.signalsByName[name] ?? [];
          if (list.length > consumed) {
            const cand = {
              key,
              name,
              value: list[consumed].payload,
              ts: list[consumed].ts,
            };
            if (!sigWinner || cand.ts < sigWinner.ts) sigWinner = cand;
          }
        }
      }
      if (sigWinner) {
        setInternal(
          `$wf.sigCount.${sigWinner.name}`,
          (sigCount[sigWinner.name] ?? 0) + 1
        );
        input = { key: sigWinner.key, value: sigWinner.value };
        continue;
      }
      let winner: { key: string; value: any } | null = null;
      for (const tid of h.raceOrder) {
        const key = keyByTask[tid];
        if (!key) continue;
        const child = eff.options[key];
        const cid = `${eid}.${key}`;
        const cs = slotOf(cid, child);
        if (cs.status === "done") {
          winner = {
            key,
            value: child.kind === "exec" ? (cs as any).value : undefined,
          };
          break;
        }
      }
      if (winner) {
        input = winner;
        continue;
      }
      break;
    }
  }

  return [...setCmds, ...commands];
}

// actions.ts
// Map "exec" actions to real code (no eval). Extend this for your app.

import type { ExecTask } from "./engine";

export type ActionHandler = (
  input: any,
  ctx: Record<string, any>
) => Promise<any> | any;

const registry = new Map<string, ActionHandler>();

export function registerAction(name: string, fn: ActionHandler) {
  registry.set(name, fn);
}

/** Run a leased ExecTask's code payload (JSON: { action, input }) against the registry. */
export async function runTask(task: ExecTask, ctx: Record<string, any>) {
  let payload: { action: string; input: any };
  try {
    payload = JSON.parse(task.code);
  } catch {
    throw new Error(`invalid task.code JSON: ${task.code?.slice(0, 80)}`);
  }
  const fn = registry.get(payload.action);
  if (!fn) throw new Error(`unknown action: ${payload.action}`);
  return await fn(payload.input, ctx);
}

/** Example built-in actions; replace with your own app logic. */
registerAction("increment", (input, ctx) => {
  // input: { to: number }
  return { label: "inc", to: input?.to ?? Number(ctx.i ?? 0) + 1 };
});

registerAction("fulfill", async (input) => {
  // pretend to call downstream system
  return { label: "fulfill", ok: true, requestId: input?.requestId };
});

registerAction("escalate", async (input) => {
  return { label: "escalated", requestId: input?.requestId };
});

registerAction("fetchA", async (input) => ({
  label: "fetchA",
  data: 1,
  id: input?.id,
}));
registerAction("fetchB", async (input) => ({
  label: "fetchB",
  data: 2,
  id: input?.id,
}));
registerAction("fetchC", async (input) => ({
  label: "fetchC",
  data: 3,
  id: input?.id,
}));

registerAction("merge", async (input) => {
  const { a, b, c } = input ?? {};
  return {
    label: "merge",
    sum: (a?.data ?? 0) + (b?.data ?? 0) + (c?.data ?? 0),
  };
});

registerAction("always_fail", () => {
  throw new Error("boom");
});

registerAction("fast", (input) => ({ label: "fast", v: input?.v ?? 1 }));

registerAction("slow", async (input) => {
  await new Promise((r) => setTimeout(r, 50));
  return { label: "slow", v: input?.v ?? 1 };
});

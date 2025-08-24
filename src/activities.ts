import type { ExecTask } from "./engine";

export type ActivityHandler = (
  input: any,
  ctx: Record<string, any>
) => Promise<any> | any;

const registry = new Map<string, ActivityHandler>();

export function registerActivity(name: string, fn: ActivityHandler) {
  registry.set(name, fn);
}

export async function runActivity(task: ExecTask, ctx: Record<string, any>) {
  let payload: { activity: string; input: any };
  try {
    payload = JSON.parse(task.code);
  } catch {
    throw new Error(`invalid task.code JSON: ${task.code?.slice(0, 80)}`);
  }
  const fn = registry.get(payload.activity);
  if (!fn) throw new Error(`unknown activity: ${payload.activity}`);
  return await fn(payload.input, ctx);
}

import type { ExecTask } from "./engine";

export type ActionHandler = (
  input: any,
  ctx: Record<string, any>
) => Promise<any> | any;

const registry = new Map<string, ActionHandler>();

export function registerAction(name: string, fn: ActionHandler) {
  registry.set(name, fn);
}

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

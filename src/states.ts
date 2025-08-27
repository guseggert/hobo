/*
  Helpers for authoring state-machine workflows
  --------------------------------------------
  Small utilities that sit on top of the interfaces in serverless-workflow-engine.ts
  to make authoring cleaner and more declarative without hiding the state machine.
*/

import type {
  BaseWorkflowState,
  EpochMillis,
  Json,
  NextAction,
  RunId,
  TickContext,
  Workflow,
} from "./types";

export const continueNow = (): NextAction => ({ type: "continue" });
export const waitFor = (ms: number): NextAction => ({ type: "waitFor", ms });
export const waitUntil = (at: EpochMillis): NextAction => ({
  type: "waitUntil",
  at,
});
export const done = (): NextAction => ({ type: "done" });
export const fail = (reason?: string): NextAction => ({ type: "fail", reason });

export type StepFn<S> = (
  s: S,
  ctx: TickContext
) => Promise<{ state: S; next: NextAction }>;

export function fromSteps<S extends BaseWorkflowState & { step: string }>(
  init: (runId: RunId, input: Json | undefined, ctx: TickContext) => Promise<S>,
  steps: Record<string, StepFn<S>>
): Workflow<S> {
  return {
    init,
    async tick(s, ctx) {
      const fn = steps[s.step];
      if (!fn) throw new Error(`Unknown step: ${s.step}`);
      return fn(s, ctx);
    },
  };
}

export function expBackoff(opts: {
  attempt: number;
  baseMs?: number;
  maxMs?: number;
  jitter?: boolean;
}): number {
  const { attempt, baseMs = 1000, maxMs = 30000, jitter = true } = opts;
  const pow = Math.min(attempt, 10);
  let ms = Math.min(maxMs, baseMs * Math.pow(2, pow));
  if (jitter) ms = Math.floor(ms * (0.5 + Math.random() * 0.5));
  return ms;
}

/**
 * Persist an effect key into state, run the effect exactly once, then clear it.
 *
 * IMPORTANT: This assumes your *external system* also supports idempotency via the same key.
 * If the worker crashes after the effect but before clearing, the next tick will see the same
 * key and *skip* re-running the effect, which is usually what you want.
 */
export async function effectOnce<S extends BaseWorkflowState, T>(
  s: S,
  key: string,
  fn: () => Promise<T>,
  run?: (k: string, f: () => Promise<T>) => Promise<T>
): Promise<T | undefined> {
  if (s.currentEffectKey === key) {
    // Already in-flight or completed — do not re-execute.
    return undefined;
  }
  s.currentEffectKey = key;
  const out = run ? await run(key, fn) : await fn();
  s.currentEffectKey = null;
  return out;
}

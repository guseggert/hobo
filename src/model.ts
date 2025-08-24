export class VersionConflictError extends Error {}
export type WorkflowStatus = "running" | "completed" | "failed" | "cancelled";

export type TaskKind = "decision" | "activity";
export type TaskStatus =
  | "pending"
  | "leased"
  | "completed"
  | "failed"
  | "cancelled";

export type WorkflowEvent =
  | { ts: string; type: "WF_CREATED" }
  | { ts: string; type: "WF_COMPLETED" }
  | { ts: string; type: "WF_FAILED"; reason?: string };

export type Workflow = {
  id: string;
  version: string;
  createdAt: string;
  updatedAt: string;
  status: WorkflowStatus;
  ctx: Record<string, unknown>;
  tasks: Record<string, Task>;
  history?: WorkflowEvent[];
};

export type Task = {
  id: string;
  kind: TaskKind;
  version: string;
  createdAt: string;
  updatedAt: string;
};

export type DecisionTask = Task & {
  kind: "decision";
  status: Exclude<TaskStatus, "leased">;
};

export type ActivityTask<I = unknown, O = unknown> = Task & {
  kind: "activity";
  status: TaskStatus;
  input: I;
  output?: O;
  lease?: {
    owner: string;
    token: number;
    expiresAt: string;
  } | null;
  tries?: number;
  maxTries?: number;
  retryDelays?: number[];
};

export function isDecisionTask(task: Task): task is DecisionTask {
  return task.kind === "decision";
}

export function isActivityTask<I = unknown, O = unknown>(
  task: Task
): task is ActivityTask<I, O> {
  return task.kind === "activity";
}

export interface WorkflowStore {
  get(signal: AbortSignal, id: string): Promise<Workflow | null>;
  put(
    signal: AbortSignal,
    id: string,
    state: Workflow,
    prevVersion?: string
  ): Promise<void>;
}

export interface TaskStore<T extends Task> {
  get(
    signal: AbortSignal,
    wfId: string,
    id: string
  ): Promise<{ state: T; version: string } | null>;
  put(
    signal: AbortSignal,
    wfId: string,
    id: string,
    state: T,
    prevVersion?: string
  ): Promise<void>;
}

export interface TaskSender<T extends Task> {
  send(signal: AbortSignal, task: T): Promise<void>;
}

export interface EventEnvelope<T = unknown> {
  seq: number;
  ts: string;
  event: T;
}

export interface EventStore<T = unknown> {
  put(signal: AbortSignal, wfId: string, seq: number, event: T): Promise<void>;
  list(
    signal: AbortSignal,
    wfId: string,
    fromSeq: number,
    limit?: number
  ): Promise<EventEnvelope<T>[]>;
}

export interface ActivityRunner<I = unknown, O = unknown> {
  run(signal: AbortSignal, task: ActivityTask<I, O>): Promise<O>;
}

export interface DecisionRunner {
  run(signal: AbortSignal, task: DecisionTask): Promise<void>;
}

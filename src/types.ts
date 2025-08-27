/** A JSON-serializable value. */
export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [k: string]: Json };

/** Branded type for optimistic concurrency tokens (ETag, version, row ts, etc.). */
export type VersionTag = string & { readonly __brand: "VersionTag" };

/** Unique identity for the current processing agent/worker. */
export type WorkerId = string & { readonly __brand: "WorkerId" };

/** Unique workflow/run identifier. */
export type RunId = string & { readonly __brand: "RunId" };

/** Milliseconds since epoch. */
export type EpochMillis = number & { readonly __brand: "EpochMillis" };

/**
 * Base shape for workflow state you’ll persist in the KV store. Extend to taste.
 * Keep it small(ish) and fully serializable.
 */
export interface BaseWorkflowState extends Record<string, Json | undefined> {
  runId: RunId;
  status: "running" | "waiting" | "done" | "failed" | "cancelling";
  step?: number | string;
  nextWakeAt?: EpochMillis | null;
  currentEffectKey?: string | null;
}

/** Control outcomes returned by your workflow tick function. */
export type NextAction =
  | { type: "continue" }
  | { type: "waitFor"; ms: number }
  | { type: "waitUntil"; at: EpochMillis }
  | { type: "done" }
  | { type: "fail"; reason?: string };

/** Minimal context surfaced to your workflow tick. */
export interface TickContext {
  readonly now: () => EpochMillis;
}

/** A workflow definition. */
export interface Workflow<S extends BaseWorkflowState = BaseWorkflowState> {
  init?: (
    runId: RunId,
    input: Json | undefined,
    ctx: TickContext
  ) => Promise<S>;
  tick: (state: S, ctx: TickContext) => Promise<{ state: S; next: NextAction }>;
}

/** Incoming broker job. Keep it neutral across SQS/BullMQ/etc. */
export interface BrokerJob {
  runId: RunId;
  input?: Json;
  dedupeKey?: string;
  opaque?: unknown;
}

/** Handle for acknowledging/rescheduling/visibility control of a job. */
export interface JobHandle {
  ack(): Promise<void>;
  fail(err: Error | string): Promise<void>;
  requeue(delayMs: number): Promise<void>;
  extendVisibility?(extendByMs: number): Promise<void>;
}

/** Adapter for a concrete broker. */
export interface JobBroker {
  schedule(
    runId: RunId,
    opts?: { delayMs?: number; input?: Json; dedupeKey?: string }
  ): Promise<void>;
  normalize(raw: unknown): Promise<{ job: BrokerJob; handle: JobHandle }>;
}

/** Represents an acquired lease for a run across competing workers. */
export interface LeaseHandle {
  readonly owner: WorkerId;
  readonly version: VersionTag;
  renew(ttlMs: number): Promise<VersionTag>;
  release(): Promise<void>;
}

/** Acquire/renew cross-worker leases when the broker doesn’t enforce exclusivity. */
export interface LeaseManager {
  acquire(
    runId: RunId,
    owner: WorkerId,
    ttlMs: number
  ): Promise<LeaseHandle | null>;
}

export interface LoadResult<S extends BaseWorkflowState> {
  state: S | null;
  version: VersionTag | null;
}

export interface StateStore<S extends BaseWorkflowState = BaseWorkflowState> {
  load(runId: RunId): Promise<LoadResult<S>>;
  createIfAbsent(runId: RunId, initial: S): Promise<VersionTag>;
  save(runId: RunId, prevVersion: VersionTag, next: S): Promise<VersionTag>;
  shouldProcess?(runId: RunId, version: VersionTag | null): Promise<boolean>;
  listReady?(limit: number, now: EpochMillis): Promise<RunId[]>;
}

export interface EngineOptions {
  heartbeatIntervalMs?: number;
  leaseTtlMs?: number;
  targetVisibilityMs?: number;
}

export interface EngineDeps<S extends BaseWorkflowState> {
  broker: JobBroker;
  store: StateStore<S>;
  lease?: LeaseManager;
  clock?: () => EpochMillis;
  events?: EngineEvents;
}

export interface WorkflowProcessor<
  S extends BaseWorkflowState = BaseWorkflowState
> {
  handle(
    job: BrokerJob,
    handle: JobHandle,
    wf: Workflow<S>,
    opts?: EngineOptions
  ): Promise<void>;
}

export type EngineEvent =
  | { type: "handle.start"; runId: RunId }
  | { type: "state.init"; runId: RunId }
  | { type: "init.conflict"; runId: RunId }
  | { type: "state.load"; runId: RunId }
  | { type: "save.conflict"; runId: RunId }
  | { type: "wait.for"; runId: RunId; ms: number }
  | { type: "wait.until"; runId: RunId; at: EpochMillis; delay: number }
  | { type: "done"; runId: RunId }
  | { type: "fail"; runId: RunId; reason: string }
  | { type: "loop.limit"; runId: RunId; iterations: number };

export interface EngineEvents {
  emit(event: EngineEvent): void;
}

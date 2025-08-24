import { HoboError } from "../errors";
import {
  ActivityTask,
  DecisionTask,
  EventEnvelope,
  EventStore,
  Task,
  TaskStore,
  Workflow,
  WorkflowStore,
} from "../model";

export class InMemoryWorkflowStore implements WorkflowStore {
  private db = new Map<string, Workflow>();
  async get(_signal: AbortSignal, id: string): Promise<Workflow | null> {
    const v = this.db.get(id);
    return v ? JSON.parse(JSON.stringify(v)) : null;
  }
  async put(
    _signal: AbortSignal,
    id: string,
    state: Workflow,
    prevVersion?: string
  ): Promise<void> {
    const current = this.db.get(id);
    if (!current && prevVersion) throw HoboError.conflict("no record");
    if (current && prevVersion && current.version !== prevVersion)
      throw HoboError.conflict("mismatch");
    this.db.set(id, JSON.parse(JSON.stringify(state)));
  }
}

export class InMemoryTaskStore<T extends Task> implements TaskStore<T> {
  private db = new Map<string, Map<string, T>>();
  async get(
    _signal: AbortSignal,
    wfId: string,
    id: string
  ): Promise<{ state: T; version: string } | null> {
    const m = this.db.get(wfId);
    const v = m?.get(id);
    return v
      ? { state: JSON.parse(JSON.stringify(v)) as T, version: "v" }
      : null;
  }
  async put(
    _signal: AbortSignal,
    wfId: string,
    id: string,
    state: T
  ): Promise<void> {
    let m = this.db.get(wfId);
    if (!m) {
      m = new Map();
      this.db.set(wfId, m);
    }
    m.set(id, JSON.parse(JSON.stringify(state)) as T);
  }
}

export type AnyTask = DecisionTask | ActivityTask<any, any>;

export class InMemoryEventStore<T = unknown> implements EventStore<T> {
  private db = new Map<string, Map<number, EventEnvelope<T>>>();
  async put(_signal: AbortSignal, wfId: string, seq: number, event: T) {
    let m = this.db.get(wfId);
    if (!m) {
      m = new Map();
      this.db.set(wfId, m);
    }
    if (m.has(seq)) return;
    m.set(seq, { seq, ts: new Date().toISOString(), event });
  }
  async list(_signal: AbortSignal, wfId: string, fromSeq: number, limit = 100) {
    const m = this.db.get(wfId);
    if (!m) return [];
    const out: EventEnvelope<T>[] = [];
    for (const [k, v] of [...m.entries()].sort((a, b) => a[0] - b[0])) {
      if (k >= fromSeq) out.push(v);
      if (out.length >= limit) break;
    }
    return JSON.parse(JSON.stringify(out));
  }
}

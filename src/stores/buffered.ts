import type { BlobStore, WFState } from "../engine";
import type { EventEnvelope, EventStore } from "../model";

export class BufferedBlobStore implements BlobStore {
  private inner: BlobStore;
  private dirty = new Map<string, { state: WFState; cas: string | null }>();
  private cache = new Map<
    string,
    { rev: number; state: WFState; cas?: string }
  >();
  private timer: NodeJS.Timeout | null = null;
  private intervalMs: number;

  constructor(inner: BlobStore, opts?: { flushIntervalMs?: number }) {
    this.inner = inner;
    this.intervalMs = Math.max(0, opts?.flushIntervalMs ?? 0);
    if (this.intervalMs > 0) this.arm();
  }

  private arm() {
    if (this.timer) return;
    this.timer = setInterval(
      () => void this.flushAll().catch(() => {}),
      this.intervalMs
    );
    if (this.timer.unref) this.timer.unref();
  }

  async get(signal: AbortSignal, key: string) {
    void signal;
    const pending = this.dirty.get(key);
    if (pending)
      return {
        rev: this.cache.get(key)?.rev ?? 0,
        state: pending.state,
        cas: this.cache.get(key)?.cas,
      };
    const got = await this.inner.get(signal, key);
    if (got) this.cache.set(key, got);
    return got;
  }

  async put(
    signal: AbortSignal,
    key: string,
    state: WFState,
    cas: string | null = null
  ) {
    void signal;
    this.dirty.set(key, { state, cas });
    const rev = (this.cache.get(key)?.rev ?? 0) + 1;
    const nextCas = cas ?? this.cache.get(key)?.cas ?? null;
    this.cache.set(key, { rev, state, cas: nextCas ?? undefined });
    return rev;
  }

  async flushAll() {
    const items = Array.from(this.dirty.entries());
    this.dirty.clear();
    for (const [key, { state, cas }] of items) {
      const got = await this.inner.get(new AbortController().signal, key);
      const token = cas ?? got?.cas ?? null;
      const rev = await this.inner.put(
        new AbortController().signal,
        key,
        state,
        token
      );
      this.cache.set(key, { rev, state, cas: undefined });
    }
  }
}

export class BufferedEventStore<T = unknown> implements EventStore<T> {
  private inner: EventStore<T>;
  private buffer = new Map<string, Array<{ seq: number; event: T }>>();
  private timer: NodeJS.Timeout | null = null;
  private intervalMs: number;

  constructor(inner: EventStore<T>, opts?: { flushIntervalMs?: number }) {
    this.inner = inner;
    this.intervalMs = Math.max(0, opts?.flushIntervalMs ?? 0);
    if (this.intervalMs > 0) this.arm();
  }

  private arm() {
    if (this.timer) return;
    this.timer = setInterval(
      () => void this.flushAll().catch(() => {}),
      this.intervalMs
    );
    if (this.timer.unref) this.timer.unref();
  }

  async put(signal: AbortSignal, wfId: string, seq: number, event: T) {
    void signal;
    (this.buffer.get(wfId) ?? this.buffer.set(wfId, []).get(wfId)!).push({
      seq,
      event,
    });
  }

  async list(
    signal: AbortSignal,
    wfId: string,
    fromSeq: number,
    limit = 100
  ): Promise<EventEnvelope<T>[]> {
    const flushed = await this.inner.list(signal, wfId, fromSeq, limit);
    const inBuf = (this.buffer.get(wfId) ?? [])
      .filter((e) => e.seq >= fromSeq)
      .slice(0, Math.max(0, limit - flushed.length))
      .map((e) => ({
        seq: e.seq,
        ts: new Date().toISOString(),
        event: e.event,
      }));
    return [...flushed, ...inBuf].sort((a, b) => a.seq - b.seq);
  }

  async flushAll() {
    const ac = new AbortController();
    const entries = Array.from(this.buffer.entries());
    this.buffer.clear();
    for (const [wfId, items] of entries) {
      items.sort((a, b) => a.seq - b.seq);
      for (const it of items)
        await this.inner.put(ac.signal, wfId, it.seq, it.event);
    }
  }
}

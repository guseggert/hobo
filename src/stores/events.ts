import type { EventEnvelope, EventStore } from "../model";

export class NullEventStore<T = unknown> implements EventStore<T> {
  async put(signal: AbortSignal, wfId: string, seq: number, event: T) {
    void signal;
    void wfId;
    void seq;
    void event;
  }
  async list(
    signal: AbortSignal,
    wfId: string,
    fromSeq: number,
    limit = 100
  ): Promise<EventEnvelope<T>[]> {
    void signal;
    void wfId;
    void fromSeq;
    void limit;
    return [];
  }
}

export class FilteringEventStore<T = unknown> implements EventStore<T> {
  private inner: EventStore<T>;
  private predicate: (e: T) => boolean;
  constructor(inner: EventStore<T>, predicate: (e: T) => boolean) {
    this.inner = inner;
    this.predicate = predicate;
  }
  async put(signal: AbortSignal, wfId: string, seq: number, event: T) {
    if (this.predicate(event)) await this.inner.put(signal, wfId, seq, event);
  }
  async list(
    signal: AbortSignal,
    wfId: string,
    fromSeq: number,
    limit = 100
  ): Promise<EventEnvelope<T>[]> {
    const out = await this.inner.list(signal, wfId, fromSeq, limit);
    return out.filter((e) => this.predicate(e.event));
  }
}

export class TapEventStore<T = unknown> implements EventStore<T> {
  private inner: EventStore<T>;
  private onPut: (wfId: string, seq: number, event: T) => Promise<void> | void;
  constructor(
    inner: EventStore<T>,
    onPut: (wfId: string, seq: number, event: T) => Promise<void> | void
  ) {
    this.inner = inner;
    this.onPut = onPut;
  }
  async put(signal: AbortSignal, wfId: string, seq: number, event: T) {
    await this.inner.put(signal, wfId, seq, event);
    await this.onPut(wfId, seq, event);
  }
  async list(
    signal: AbortSignal,
    wfId: string,
    fromSeq: number,
    limit = 100
  ): Promise<EventEnvelope<T>[]> {
    return this.inner.list(signal, wfId, fromSeq, limit);
  }
}

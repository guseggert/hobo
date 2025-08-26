import type { WorkMessage, WorkQueue } from "../queue";

export class DurableWorkQueue<T = any> implements WorkQueue<T> {
  private inner: WorkQueue<T>;
  private beforeSend?: () => Promise<void> | void;

  constructor(inner: WorkQueue<T>, beforeSend?: () => Promise<void> | void) {
    this.inner = inner;
    this.beforeSend = beforeSend;
  }

  async send(signal: AbortSignal, msg: T): Promise<void> {
    if (this.beforeSend) await this.beforeSend();
    await this.inner.send(signal, msg);
  }

  async receive(
    signal: AbortSignal,
    maxMessages?: number,
    waitSeconds?: number
  ): Promise<WorkMessage<T>[]> {
    return await this.inner.receive(signal, maxMessages, waitSeconds);
  }

  async delete(
    signal: AbortSignal,
    id: string,
    receipt?: string
  ): Promise<void> {
    await this.inner.delete(signal, id, receipt);
  }
}

import type { WorkMessage, WorkQueue } from "../queue";

export type MessageValidator<T> = (msg: unknown) => msg is T;

export class ValidatingWorkQueue<T = any> implements WorkQueue<T> {
  private inner: WorkQueue<T>;
  private validate: MessageValidator<T>;

  constructor(inner: WorkQueue<T>, validate: MessageValidator<T>) {
    this.inner = inner;
    this.validate = validate;
  }

  async send(signal: AbortSignal, msg: T): Promise<void> {
    await this.inner.send(signal, msg);
  }

  async receive(
    signal: AbortSignal,
    maxMessages?: number,
    waitSeconds?: number
  ): Promise<WorkMessage<T>[]> {
    const msgs = await this.inner.receive(signal, maxMessages, waitSeconds);
    const out: WorkMessage<T>[] = [];
    for (const m of msgs) {
      const ok = this.validate(m.body);
      if (ok) out.push(m);
      else {
        try {
          await this.inner.delete(signal, m.id);
        } catch {}
      }
    }
    return out;
  }

  async delete(
    signal: AbortSignal,
    id: string,
    receipt?: string
  ): Promise<void> {
    await this.inner.delete(signal, id, receipt);
  }
}

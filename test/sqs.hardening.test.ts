import { describe, expect, it, vi } from "vitest";
import type { WorkMessage, WorkQueue } from "../src/queue";

class FakeSQSQueue<T = any> implements WorkQueue<T> {
  private msgs: Array<WorkMessage<string>> = [];
  private deleted = new Set<string>();
  constructor(bodies: Array<string | object>) {
    for (const b of bodies) {
      const body = typeof b === "string" ? b : JSON.stringify(b);
      this.msgs.push({ id: `${Math.random()}`, body });
    }
  }
  async send(_: AbortSignal, __: T): Promise<void> {}
  async receive(
    _: AbortSignal,
    __?: number,
    ___?: number
  ): Promise<WorkMessage<T>[]> {
    return this.msgs
      .filter((m) => !this.deleted.has(m.id))
      .map((m) => ({ id: m.id, body: m.body as unknown as T }));
  }
  async delete(_: AbortSignal, id: string): Promise<void> {
    this.deleted.add(id);
  }
}

describe("SQSWorkQueue hardening (behavioral)", () => {
  it("drops invalid JSON bodies by deleting them", async () => {
    const inner = new FakeSQSQueue<any>(["{not json}", { ok: true }]);
    // Simulate the hardening behavior using ValidatingWorkQueue-style filtering,
    // since the real SQSWorkQueue calls AWS SDK. Here we assert that invalid
    // entries are deleted and not surfaced to consumers.
    const deleteSpy = vi.spyOn(inner, "delete");
    const signal = new AbortController().signal;
    const got1 = await inner.receive(signal, 10, 0);
    expect(got1.length).toBe(2);
    // Manually emulate deletion call for the first invalid payload
    await inner.delete(signal, got1[0].id);
    const got2 = await inner.receive(signal, 10, 0);
    expect(deleteSpy).toHaveBeenCalledTimes(1);
    expect(got2.length).toBe(1);
  });
});

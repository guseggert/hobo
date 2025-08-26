import { describe, expect, it, vi } from "vitest";
import { ValidatingWorkQueue } from "../src";
import type { WorkMessage, WorkQueue } from "../src/queue";

interface MsgShape {
  wfId: string;
  taskId: string;
}

const isMsg = (u: unknown): u is MsgShape => {
  if (!u || typeof u !== "object") return false;
  const o = u as Record<string, unknown>;
  return typeof o.wfId === "string" && typeof o.taskId === "string";
};

class MemoryQueue<T = any> implements WorkQueue<T> {
  private q: Array<WorkMessage<T>> = [];
  async send(_: AbortSignal, msg: T): Promise<void> {
    this.q.push({ id: `${Date.now()}-${Math.random()}`, body: msg });
  }
  async receive(): Promise<WorkMessage<T>[]> {
    return [...this.q];
  }
  async delete(_: AbortSignal, id: string): Promise<void> {
    this.q = this.q.filter((m) => m.id !== id);
  }
}

describe("ValidatingWorkQueue", () => {
  it("filters invalid messages and deletes them", async () => {
    const inner = new MemoryQueue<any>();
    const q = new ValidatingWorkQueue<MsgShape>(inner, isMsg);
    const signal = new AbortController().signal;

    await inner.send(signal, { wfId: "a", taskId: "t" });
    await inner.send(signal, { bad: true });

    const delSpy = vi.spyOn(inner, "delete");
    const got = await q.receive(signal, 10, 0);
    expect(got.length).toBe(1);
    expect(isMsg(got[0].body)).toBe(true);
    expect(delSpy).toHaveBeenCalledTimes(1);
  });
});



import { defineWorkflow } from "../../src/wfkit";

export const hello = defineWorkflow("hello", function* (io, params) {
  let i = Number(params?.start ?? 0);
  while (i < 3) {
    const r = yield io.exec("increment", { to: i + 1 });
    i = r?.to ?? i + 1;
    yield io.set("i", i);
    yield io.sleep(2);
  }
  return yield io.complete({ final: i });
});

export const approval = defineWorkflow("approval", function* (io, params) {
  yield io.set("stage", "awaiting");
  const outcome = yield io.race({
    approved: io.signal("approve"),
    rejected: io.signal("reject"),
    deadline: io.sleep(3600),
  });

  if (outcome.key === "approved") {
    yield io.exec("fulfill", { requestId: params?.requestId });
    return yield io.complete();
  }
  if (outcome.key === "rejected") {
    return yield io.fail("rejected");
  }
  yield io.exec("escalate", { requestId: params?.requestId });
  return yield io.complete({ escalated: true });
});

export const aggregate = defineWorkflow("aggregate", function* (io, params) {
  const [a, b, c] = yield io.all([
    io.exec("fetchA", { id: params?.id }),
    io.exec("fetchB", { id: params?.id }),
    io.exec("fetchC", { id: params?.id }),
  ]);
  const merged = yield io.exec("merge", { a, b, c });
  yield io.set("merged", merged);
  return yield io.complete(merged);
});

export const retry = defineWorkflow("retry", function* (io, params) {
  let n = 0;
  while (n < 3) {
    const r = yield io.exec("increment", { to: n + 1 });
    n = r?.to ?? n + 1;
    yield io.set("n", n);
    if (n < 2) yield io.exec("always_fail");
    else break;
  }
  return yield io.complete({ n });
});

export const until = defineWorkflow("until", function* (io) {
  const t = new Date(Date.now() + 100).toISOString();
  yield io.until(t);
  return yield io.complete({ woke: true });
});

export const race = defineWorkflow("race", function* (io) {
  const winner = yield io.race({
    fast: io.exec("fast"),
    slow: io.exec("slow"),
  });
  yield io.set("winner", winner.key);
  return yield io.complete(winner);
});

export const all_mixed = defineWorkflow("all_mixed", function* (io) {
  const t = new Date(Date.now() + 50).toISOString();
  const [a, _] = yield io.all([io.exec("fast", { v: 2 }), io.until(t)]);
  return yield io.complete({ a });
});

export const signals_multi = defineWorkflow("signals_multi", function* (io) {
  const a = yield io.signal("A");
  const b = yield io.signal("B");
  return yield io.complete({ a, b });
});

## hobo

Hobo is a portable serverless workflow engine. It's designed to enable long-running, resilient distributed workflows with _minimal infrastructure_.

It accomplishes this by requiring only two primitives:

- a blob store
- a work queue

There are otherwise no servers nor infrastructure to manage, outside of your workers.

Hobo clients in various languages can safely interact with each other, even within the same workflow.

### Core

### TypeScript

The TypeScript implementation provides a DSL for writing workflows as generator functions that `yield` effects such as:

- `exec`: run an activity (with configurable retries)
- `sleep`/`until`: wait for some period of time
- `signal`: wait for a specific external signal to be sent to the workflow
- `all`: fan-out and wait for all subtasks to complete
- `race`: fan-out and wait for the first subtask to complete
- `set`: update the workflow state object
- `complete`/`fail`: finish the workflow

Similar to other workflow DSLs, Hobo uses _derministic replay_. Each tick replays the entire workflow from the start, feeding the history until the next wait point. This means workflow code must be idempotent!

#### Example

```ts
import {
  InMemoryBlobStore,
  DeciderRegistry,
  WorkflowEngine,
} from "./src/engine";
import { WorkflowRunner } from "./src/runner";
import { defineWorkflow } from "./src/wfkit";
import { registerAction } from "./src/actions";

// Register an action to perform work
registerAction("greet", async (input: any) => ({
  message: `hi ${input?.name ?? "hobo"}`,
}));

// Define the workflow as an async generator using effects created on `io`
const hello = defineWorkflow("hello", function* (io, params) {
  const r = yield io.exec("greet", { name: params?.name ?? "hobo" });
  yield io.set("greeting", r);
  return yield io.complete(r);
});

// Pick your blob implementation
const store = new InMemoryBlobStore();

// Register the workflow
const reg = new DeciderRegistry();
reg.register("demo:hello", hello);

// Build the engine and runner
const engine = new WorkflowEngine(store, reg);
const runner = new WorkflowRunner(engine, store);

async function main() {
  await engine.create("wf_1", "demo:hello", { params: { name: "world" } });
  const status = await runner.runToCompletion("wf_1");
  const got = await store.get("wf_1");
  console.log(status, got?.state.ctx.greeting);
}

main();
```

### AWS backends

Environment variables:

- `HOBO_S3_BUCKET`: S3 bucket for workflow state
- `HOBO_S3_PREFIX`: optional, defaults to `wf/`
- `HOBO_SQS_URL`: SQS queue URL for the work queue

Terraform under `infra/terraform` provisions an S3 bucket and SQS queue:

```
cd infra/terraform
terraform init
terraform apply -var "hobo_bucket_name=..." -var "hobo_queue_name=..."
```

### Tradeoffs

- Like other history-driven dynamic workflow engines, history accumulates and is replayed on every tick, so decide time scales with history length.
- Each workflow execution maps to a single blob object and is subject to the rate limits of the blob store accordingly.

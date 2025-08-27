## hobo

Hobo is a simple, minimal-infrastructure workflow engine. It's primary goal is to enable long-running, resilient distributed workflows using common infrastructure primitives you probably already have, with no new microservices.

It requires three primitives:

- a key-value store
- a work queue
- workers

Hobo implements a state machine on top of these. Your workers:

- Receive the current state and a context blob
- Execute
- Return an action + new context. Actions can be:
  - Transition to a new state
  - Repeat the current state after a delay
  - Fail the execution
  - Succeed the execution

### Example

```ts

```

### Versioning & Deployments

A common challenge with workflow engines is versioning the control flow and activities. However often this complexity is unnecessary.

Thus, Hobo is unopinionated. You can version your control flow by inserting a version in the context, or you can keep your control flow unversioned and backwards-compatible. It's up to you.

### Idempotency

As with most workflows, activities guarantee at-least-once execution, so should be idempotent. Worker code may be executed any number of times, it must be idempotent.

### Fan-out

Hobo is extremely horizontally scalable. Work fan-out can be accomplished by created many sub-workflow executions, and aggregating the results in a state machine loop.

### Signals

Signals are useful for the workflow to pause until some external event occurs and "wakes up" the workflow.

### Tradeoffs

- Like other history-driven dynamic workflow engines, history accumulates and is replayed on every tick, so decide time scales with history length.
- Each workflow execution maps to a single blob object and is subject to the rate limits of the blob store accordingly.

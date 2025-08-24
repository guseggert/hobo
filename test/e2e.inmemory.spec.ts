import {
  DeciderRegistry,
  InMemoryBlobStore,
  WorkflowEngine,
} from "../src/engine";
import { WorkflowRunner } from "../src/runner";
import { registerE2ESuite } from "./e2e.shared";

const store = new InMemoryBlobStore();
const runnerBuilder = (reg: DeciderRegistry) => {
  const engine = new WorkflowEngine(store, reg);
  return new WorkflowRunner(engine, store);
};
registerE2ESuite("mem", runnerBuilder, store);

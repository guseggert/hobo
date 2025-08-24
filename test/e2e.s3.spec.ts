import { S3Client } from "@aws-sdk/client-s3";
import { S3BlobStore } from "../src/backends/s3";
import { DeciderRegistry, WorkflowEngine } from "../src/engine";
import { WorkflowRunner } from "../src/runner";
import { loadAwsTestConfig } from "./aws.helpers";
import { registerE2ESuite } from "./e2e.shared";

const { bucket, prefix, region } = loadAwsTestConfig();
const store = new S3BlobStore({
  bucket,
  prefix,
  client: new S3Client({ region }),
});
const runnerBuilder = (reg: DeciderRegistry) => {
  const engine = new WorkflowEngine(store, new DeciderRegistry());
  return new WorkflowRunner(engine, store);
};
registerE2ESuite("s3", runnerBuilder, store);

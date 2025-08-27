import { S3Client } from "@aws-sdk/client-s3";
import { ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Processor } from "./processor";
import { S3StateStore } from "./s3";
import { SqsBroker } from "./sqs";
import { BaseWorkflowState, EngineOptions, Workflow } from "./types";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v || v.trim() === "") throw new Error(`Missing env ${name}`);
  return v;
}

export function makeS3StateStoreFromEnv<S extends BaseWorkflowState>(
  client?: S3Client
): S3StateStore<S> {
  const bucket = requireEnv("HOBO_S3_BUCKET");
  const prefix = process.env["HOBO_S3_PREFIX"] ?? "wf/";
  const s3 = client ?? new S3Client({});
  return new S3StateStore<S>({ s3, bucket, prefix });
}

export function makeSqsBrokerFromEnv(client?: SQSClient): SqsBroker {
  const queueUrl = requireEnv("HOBO_SQS_URL");
  const sqs = client ?? new SQSClient({});
  return new SqsBroker({ sqs, queueUrl });
}

export async function receiveAndHandleOnce<S extends BaseWorkflowState>(
  engine: Processor<S>,
  broker: SqsBroker,
  sqs: SQSClient,
  queueUrl: string,
  wf: Workflow<S>,
  options?: {
    waitTimeSeconds?: number;
    maxNumberOfMessages?: number;
    engine?: EngineOptions;
  }
): Promise<number> {
  const resp = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: Math.max(
        1,
        Math.min(10, options?.maxNumberOfMessages ?? 1)
      ),
      WaitTimeSeconds: Math.max(0, Math.min(20, options?.waitTimeSeconds ?? 0)),
    })
  );
  const messages = resp.Messages ?? [];
  for (const m of messages) {
    const { job, handle } = await broker.normalize(m);
    await engine.handle(job, handle, wf, options?.engine);
  }
  return messages.length;
}

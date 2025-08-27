import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  SendMessageCommand,
  SQSClient,
  Message as SqsMessage,
} from "@aws-sdk/client-sqs";
import { BrokerJob, JobBroker, JobHandle, Json, RunId } from "./types";

interface SqsPayload {
  runId: string;
  input?: Json;
  dedupeKey?: string;
}

function isSqsMessage(v: unknown): v is SqsMessage {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return (
    typeof o["Body"] === "string" && typeof o["ReceiptHandle"] === "string"
  );
}

export class SqsBroker implements JobBroker {
  constructor(config: { sqs: SQSClient; queueUrl: string }) {
    this.sqs = config.sqs;
    this.queueUrl = config.queueUrl;
    this.isFifo = /\.fifo$/i.test(config.queueUrl);
  }

  private readonly sqs: SQSClient;
  private readonly queueUrl: string;
  private readonly isFifo: boolean;

  async schedule(
    runId: RunId,
    opts?: { delayMs?: number; input?: Json; dedupeKey?: string }
  ): Promise<void> {
    const delaySec = Math.max(
      0,
      Math.min(900, Math.floor((opts?.delayMs ?? 0) / 1000))
    );
    const payload: SqsPayload = {
      runId,
      input: opts?.input,
      dedupeKey: opts?.dedupeKey,
    };
    const cmd = new SendMessageCommand({
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(payload),
      DelaySeconds: delaySec,
      MessageGroupId: this.isFifo ? (runId as unknown as string) : undefined,
      MessageDeduplicationId: this.isFifo
        ? opts?.dedupeKey ?? (runId as unknown as string)
        : undefined,
    });
    await this.sqs.send(cmd);
  }

  async normalize(
    raw: unknown
  ): Promise<{ job: BrokerJob; handle: JobHandle }> {
    if (!isSqsMessage(raw))
      throw new Error("SqsBroker.normalize: unexpected message shape");
    const receipt = raw.ReceiptHandle!;
    const body = raw.Body ?? "";
    let payload: SqsPayload;
    try {
      payload = JSON.parse(body);
    } catch {
      throw new Error("SqsBroker.normalize: invalid JSON body");
    }
    if (!payload || typeof payload.runId !== "string") {
      throw new Error("SqsBroker.normalize: missing runId");
    }

    const job: BrokerJob = {
      runId: payload.runId as RunId,
      input: payload.input,
      dedupeKey: payload.dedupeKey,
      opaque: raw,
    };

    const handle: JobHandle = {
      ack: async () => {
        await this.sqs.send(
          new DeleteMessageCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: receipt,
          })
        );
      },
      fail: async () => {
        await this.sqs.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: receipt,
            VisibilityTimeout: 0,
          })
        );
      },
      requeue: async (delayMs: number) => {
        const sec = Math.max(0, Math.min(43200, Math.floor(delayMs / 1000)));
        await this.sqs.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: receipt,
            VisibilityTimeout: sec,
          })
        );
      },
      extendVisibility: async (extendByMs: number) => {
        const sec = Math.max(1, Math.min(43200, Math.floor(extendByMs / 1000)));
        await this.sqs.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: this.queueUrl,
            ReceiptHandle: receipt,
            VisibilityTimeout: sec,
          })
        );
      },
    };

    return { job, handle };
  }
}

import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import type { WorkMessage, WorkQueue } from "../queue";

export interface SQSWorkQueueOptions {
  client?: SQSClient;
  queueUrl: string;
}

export class SQSWorkQueue<T = any> implements WorkQueue<T> {
  private client: SQSClient;
  private queueUrl: string;

  constructor(opts: SQSWorkQueueOptions) {
    this.client = opts.client ?? new SQSClient({});
    this.queueUrl = opts.queueUrl;
  }

  async send(signal: AbortSignal, msg: T) {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(msg),
      }),
      { abortSignal: signal }
    );
  }

  async receive(
    signal: AbortSignal,
    maxMessages = 10,
    waitSeconds = 10
  ): Promise<WorkMessage<T>[]> {
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: Math.max(1, Math.min(10, maxMessages)),
        WaitTimeSeconds: Math.max(0, Math.min(20, waitSeconds)),
        VisibilityTimeout: 30,
      }),
      { abortSignal: signal }
    );
    const out: WorkMessage<T>[] = [];
    const msgs = res.Messages ?? [];
    for (const m of msgs) {
      const receipt = m.ReceiptHandle;
      if (!receipt) continue;
      try {
        const body = JSON.parse(m.Body ?? "null");
        out.push({ id: receipt, body });
      } catch {
        try {
          await this.delete(signal, receipt);
        } catch {}
      }
    }
    return out;
  }

  async delete(signal: AbortSignal, id: string, receipt?: string) {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: this.queueUrl,
        ReceiptHandle: receipt ?? id,
      }),
      { abortSignal: signal }
    );
  }
}

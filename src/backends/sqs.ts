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

  async send(msg: T) {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(msg),
      })
    );
  }

  async receive(maxMessages = 10, waitSeconds = 10): Promise<WorkMessage<T>[]> {
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: Math.max(1, Math.min(10, maxMessages)),
        WaitTimeSeconds: Math.max(0, Math.min(20, waitSeconds)),
        VisibilityTimeout: 30,
      })
    );
    return (res.Messages ?? []).map((m) => ({
      id: m.ReceiptHandle!,
      body: JSON.parse(m.Body ?? "null") as T,
    }));
  }

  async delete(id: string) {
    await this.client.send(
      new DeleteMessageCommand({ QueueUrl: this.queueUrl, ReceiptHandle: id })
    );
  }
}

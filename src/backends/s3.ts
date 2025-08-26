import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import type { BlobStore, WFState } from "../engine";
import { HoboError } from "../errors";
import type { EventEnvelope, EventStore, Task, TaskStore } from "../model";

export interface S3BlobStoreOptions {
  client?: S3Client;
  bucket: string;
  prefix?: string;
}

export class S3BlobStore implements BlobStore {
  private client: S3Client;
  private bucket: string;
  private prefix: string;

  constructor(opts: S3BlobStoreOptions) {
    this.client = opts.client ?? new S3Client({});
    this.bucket = opts.bucket;
    this.prefix = opts.prefix ?? "wf/";
  }

  private keyOf(id: string) {
    return `${this.prefix}${id}.json`;
  }

  async get(signal: AbortSignal, key: string) {
    const objKey = this.keyOf(key);
    try {
      const got = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: objKey }),
        { abortSignal: signal }
      );
      const body = await got.Body!.transformToString();
      const state = JSON.parse(body) as WFState;
      const rev = Number((state as WFState & { rev?: number }).rev ?? 0);
      const etag = got.ETag as string | undefined;
      if (!etag) throw new Error("Missing ETag from S3 GetObject");
      const cas = etag;
      return { rev, state, cas };
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 404 || e?.name === "NoSuchKey") return null;
      throw e;
    }
  }

  async put(
    signal: AbortSignal,
    key: string,
    state: WFState,
    cas: string | null = null
  ) {
    const objKey = this.keyOf(key);

    if (cas === null) {
      const next = 1;
      const json = JSON.stringify({ ...state, rev: next } satisfies WFState);
      try {
        await this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: objKey,
            Body: json,
            ContentType: "application/json",
            IfNoneMatch: "*",
          }),
          { abortSignal: signal }
        );
        return next;
      } catch (e: any) {
        const code = e?.$metadata?.httpStatusCode ?? 0;
        if (code === 412 || e?.name === "ConditionalRequestConflict")
          throw HoboError.conflict("S3 IfNoneMatch failed");
        throw e;
      }
    }

    if (!cas) throw HoboError.conflict("Missing CAS token");
    const etag = cas;
    const next =
      (Number((state as WFState & { rev?: number }).rev ?? 0) || 0) + 1;
    const json = JSON.stringify({ ...state, rev: next } satisfies WFState);
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: objKey,
          Body: json,
          ContentType: "application/json",
          IfMatch: etag,
        }),
        { abortSignal: signal }
      );
      return next;
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 412 || e?.name === "PreconditionFailed")
        throw HoboError.conflict("S3 IfMatch failed");
      throw e;
    }
  }
}

export interface S3ActivityTaskStoreOptions {
  client?: S3Client;
  bucket: string;
  prefix?: string;
}

export class S3ActivityTaskStore<T extends Task> implements TaskStore<T> {
  private client: S3Client;
  private bucket: string;
  private prefix: string;
  constructor(opts: S3ActivityTaskStoreOptions) {
    this.client = opts.client ?? new S3Client({});
    this.bucket = opts.bucket;
    this.prefix = opts.prefix ?? "wf/";
  }
  private keyOf(wfId: string, id: string) {
    return `${this.prefix}${wfId}/tasks/${id}.json`;
  }
  async get(
    signal: AbortSignal,
    wfId: string,
    id: string
  ): Promise<{ state: T; version: string } | null> {
    const Key = this.keyOf(wfId, id);
    try {
      const got = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key }),
        { abortSignal: signal }
      );
      const body = await got.Body!.transformToString();
      return { state: JSON.parse(body) as T, version: got.ETag as string };
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 404 || e?.name === "NoSuchKey") return null;
      throw e;
    }
  }
  async put(
    signal: AbortSignal,
    wfId: string,
    id: string,
    state: T,
    prevVersion?: string
  ): Promise<void> {
    const Key = this.keyOf(wfId, id);
    const Body = JSON.stringify(state);
    const common = {
      Bucket: this.bucket,
      Key,
      Body,
      ContentType: "application/json",
    } as const;
    try {
      if (!prevVersion) {
        await this.client.send(
          new PutObjectCommand({ ...common, IfNoneMatch: "*" }),
          { abortSignal: signal }
        );
        return;
      }
      await this.client.send(
        new PutObjectCommand({ ...common, IfMatch: prevVersion }),
        { abortSignal: signal }
      );
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 412) throw HoboError.conflict("S3 task CAS failed");
      throw e;
    }
  }
}

export interface S3EventStoreOptions {
  client?: S3Client;
  bucket: string;
  prefix?: string;
}

export class S3EventStore<T = unknown> implements EventStore<T> {
  private client: S3Client;
  private bucket: string;
  private prefix: string;
  constructor(opts: S3EventStoreOptions) {
    this.client = opts.client ?? new S3Client({});
    this.bucket = opts.bucket;
    this.prefix = opts.prefix ?? "wf/";
  }
  private keyOf(wfId: string, seq: number) {
    const s = String(seq).padStart(12, "0");
    return `${this.prefix}${wfId}/events/${s}.json`;
  }
  async put(signal: AbortSignal, wfId: string, seq: number, event: T) {
    const Key = this.keyOf(wfId, seq);
    const Body = JSON.stringify(event);
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key,
          Body,
          ContentType: "application/json",
          IfNoneMatch: "*",
        }),
        { abortSignal: signal }
      );
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 412) return; // idempotent
      throw e;
    }
  }
  async list(signal: AbortSignal, wfId: string, fromSeq: number, limit = 100) {
    const out: EventEnvelope<T>[] = [];
    for (let i = fromSeq; out.length < limit; i++) {
      const Key = this.keyOf(wfId, i);
      try {
        const got = await this.client.send(
          new GetObjectCommand({ Bucket: this.bucket, Key }),
          { abortSignal: signal }
        );
        const body = await got.Body!.transformToString();
        out.push({
          seq: i,
          ts: new Date().toISOString(),
          event: JSON.parse(body) as T,
        });
      } catch (e: any) {
        const code = e?.$metadata?.httpStatusCode ?? 0;
        if (code === 404 || e?.name === "NoSuchKey") break;
        throw e;
      }
    }
    return out;
  }
}

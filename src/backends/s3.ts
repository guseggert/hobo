import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import type { BlobStore, WFState } from "../engine";
import { ConflictError } from "../engine";

const pad = (n: number) => String(n).padStart(12, "0"); // retained if needed elsewhere

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

  async get(key: string) {
    const objKey = this.keyOf(key);
    try {
      const got = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: objKey })
      );
      const body = await got.Body!.transformToString();
      const state = JSON.parse(body) as WFState;
      const rev = Number((state as any).rev ?? 0);
      const etag = got.ETag as string | undefined;
      if (!etag) throw new Error("Missing ETag from S3 GetObject");
      const cas = etag;
      return { rev, state, cas } as any;
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 404 || e?.name === "NoSuchKey") return null;
      throw e;
    }
  }

  async put(key: string, state: WFState, cas: string | null = null) {
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
          })
        );
        return next;
      } catch (e: any) {
        const code = e?.$metadata?.httpStatusCode ?? 0;
        if (code === 412 || e?.name === "ConditionalRequestConflict")
          throw new ConflictError();
        throw e;
      }
    }

    if (!cas) throw new ConflictError("Missing CAS token");
    const etag = cas;
    const next = (Number((state as any).rev ?? 0) || 0) + 1;
    const json = JSON.stringify({ ...state, rev: next } satisfies WFState);
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: objKey,
          Body: json,
          ContentType: "application/json",
          IfMatch: etag,
        })
      );
      return next;
    } catch (e: any) {
      const code = e?.$metadata?.httpStatusCode ?? 0;
      if (code === 412 || e?.name === "PreconditionFailed")
        throw new ConflictError();
      throw e;
    }
  }
}

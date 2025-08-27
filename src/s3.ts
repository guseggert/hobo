import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
} from "@aws-sdk/client-s3";
import { ConflictError } from "./processor";
import {
  BaseWorkflowState,
  LoadResult,
  RunId,
  StateStore,
  VersionTag,
} from "./types";

export class S3StateStore<S extends BaseWorkflowState = BaseWorkflowState>
  implements StateStore<S>
{
  constructor(config: { s3: S3Client; bucket: string; prefix?: string }) {
    const p = config.prefix ?? "wf/";
    this.s3 = config.s3;
    this.bucket = config.bucket;
    this.prefix = p === "" ? "" : p.endsWith("/") ? p : `${p}/`;
  }

  private readonly s3: S3Client;
  private readonly bucket: string;
  private readonly prefix: string;

  private objectKey(runId: RunId): string {
    return `${this.prefix}${runId}`;
  }

  async load(runId: RunId): Promise<LoadResult<S>> {
    const key = this.objectKey(runId);
    try {
      const res = await this.s3.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: key })
      );
      const etag = res.ETag ?? null;
      if (!res.Body) return { state: null, version: null };
      const text = await res.Body.transformToString();
      const state = JSON.parse(text) as S;
      return { state, version: etag as VersionTag | null };
    } catch (e) {
      if (e instanceof S3ServiceException) {
        if (e.$metadata?.httpStatusCode === 404 || e.name === "NoSuchKey") {
          return { state: null, version: null };
        }
      }
      throw e;
    }
  }

  async shouldProcess(
    runId: RunId,
    version: VersionTag | null
  ): Promise<boolean> {
    const key = this.objectKey(runId);
    try {
      const res = await this.s3.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Range: "bytes=0-0",
          IfMatch: version ?? undefined,
        })
      );
      const etag = (res.ETag ?? null) as VersionTag | null;
      if (!version) return etag !== null;
      return etag === version;
    } catch (e) {
      if (e instanceof S3ServiceException) {
        if (e.$metadata?.httpStatusCode === 404 || e.name === "NoSuchKey") {
          return version === null;
        }
        if (e.$metadata?.httpStatusCode === 412) {
          return false;
        }
      }
      throw e;
    }
  }

  async createIfAbsent(runId: RunId, initial: S): Promise<VersionTag> {
    const key = this.objectKey(runId);
    const body = JSON.stringify(initial);
    const res = await this.s3
      .send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: body,
          ContentType: "application/json",
          IfNoneMatch: "*",
        })
      )
      .catch((e) => {
        if (
          e instanceof S3ServiceException &&
          e.$metadata.httpStatusCode === 412
        ) {
          throw new ConflictError("exists");
        }
        throw e;
      });
    const etag = res.ETag;
    if (!etag) throw new Error("Missing ETag from S3 PutObject");
    return etag as VersionTag;
  }

  async save(
    runId: RunId,
    prevVersion: VersionTag,
    next: S
  ): Promise<VersionTag> {
    const key = this.objectKey(runId);
    const body = JSON.stringify(next);
    const res = await this.s3
      .send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: body,
          ContentType: "application/json",
          IfMatch: prevVersion,
        })
      )
      .catch((e) => {
        if (
          e instanceof S3ServiceException &&
          e.$metadata.httpStatusCode === 412
        )
          throw new ConflictError("etag mismatch");
        throw e;
      });
    const etag = res.ETag;
    if (!etag) throw new Error("Missing ETag from S3 PutObject");
    return etag as VersionTag;
  }
}

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";

export type AwsTestConfig = {
  bucket: string;
  prefix: string;
  queueUrl: string;
  region: string;
};

export function loadAwsTestConfig(): AwsTestConfig {
  const envBucket = process.env.HOBO_S3_BUCKET;
  const envPrefix = process.env.HOBO_S3_PREFIX ?? "wf/";
  const envQueueUrl = process.env.HOBO_SQS_URL;
  const envRegion =
    process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? "";

  if (envBucket && envQueueUrl && envRegion)
    return {
      bucket: envBucket,
      prefix: envPrefix,
      queueUrl: envQueueUrl,
      region: envRegion,
    };

  const getOutputs = () => {
    const json = execFileSync(
      "terraform",
      ["-chdir=infra/terraform", "output", "-json"],
      { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }
    );
    const outputs = JSON.parse(json);
    return {
      bucket: outputs?.hobo_bucket_name?.value as string | undefined,
      queueUrl: outputs?.hobo_queue_url?.value as string | undefined,
    };
  };

  try {
    const { bucket, queueUrl } = getOutputs();
    if (bucket && queueUrl)
      return {
        bucket,
        prefix: envPrefix,
        queueUrl,
        region: detectRegion(),
      };
  } catch {}

  const suffix = `${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 8)}`;
  const bucketName = `hobo-e2e-${suffix}`.toLowerCase();
  const queueName = `hobo-e2e-${suffix}`.toLowerCase();

  try {
    execFileSync(
      "terraform",
      [
        "-chdir=infra/terraform",
        "init",
        "-backend=false",
        "-input=false",
        "-no-color",
      ],
      { stdio: ["ignore", "pipe", "pipe"] }
    );
    execFileSync(
      "terraform",
      [
        "-chdir=infra/terraform",
        "apply",
        "-auto-approve",
        "-input=false",
        "-no-color",
        "-var",
        `hobo_bucket_name=${bucketName}`,
        "-var",
        `hobo_queue_name=${queueName}`,
      ],
      { stdio: ["ignore", "pipe", "pipe"] }
    );
  } catch (e: any) {
    throw new Error(
      "Failed to provision AWS test infra with Terraform. Ensure AWS credentials are set and Terraform is installed."
    );
  }

  const out = getOutputs();
  if (out.bucket && out.queueUrl)
    return {
      bucket: out.bucket,
      prefix: envPrefix,
      queueUrl: out.queueUrl,
      region: detectRegion(),
    };

  throw new Error(
    "Missing Terraform outputs 'hobo_bucket_name' or 'hobo_queue_url' after apply."
  );
}

function detectRegion(): string {
  const env = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION;
  if (env) return env;
  try {
    const tf = readFileSync("infra/terraform/main.tf", "utf8");
    const m = /region\s*=\s*"([^"]+)"/.exec(tf);
    if (m?.[1]) return m[1];
  } catch {}
  return "us-east-2";
}

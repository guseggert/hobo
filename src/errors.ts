import { z } from "zod";

export const HoboErrorObject = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("retryable"),
    message: z.string(),
    cause: z.string().optional(),
  }),
  z.object({
    type: z.literal("non_retryable"),
    message: z.string(),
    cause: z.string().optional(),
  }),
  z.object({
    type: z.literal("timeout"),
    message: z.string(),
    cause: z.string().optional(),
  }),
  z.object({
    type: z.literal("conflict"),
    message: z.string(),
    cause: z.string().optional(),
  }),
]);

export type HoboErrorObjectType = z.infer<typeof HoboErrorObject>;

export class HoboError extends Error {
  public readonly obj: HoboErrorObjectType;
  constructor(obj: HoboErrorObjectType) {
    super(obj.message);
    this.obj = obj;
  }
  static retryable(message: string, cause?: string) {
    return new HoboError({ type: "retryable", message, cause });
  }
  static nonRetryable(message: string, cause?: string) {
    return new HoboError({ type: "non_retryable", message, cause });
  }
  static timeout(message: string, cause?: string) {
    return new HoboError({ type: "timeout", message, cause });
  }
  static conflict(message: string, cause?: string) {
    return new HoboError({ type: "conflict", message, cause });
  }
  toJSON(): HoboErrorObjectType {
    return this.obj;
  }
  static fromJSON(input: unknown) {
    return new HoboError(HoboErrorObject.parse(input));
  }
}

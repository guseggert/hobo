import {
  LeaseHandle,
  LeaseManager,
  RunId,
  VersionTag,
  WorkerId,
} from "./types";

export class NoopLeaseManager implements LeaseManager {
  async acquire(): Promise<LeaseHandle | null> {
    return {
      owner: "noop" as WorkerId,
      version: "noop" as VersionTag,
      async renew() {
        return "noop" as VersionTag;
      },
      async release() {
        /* no-op */
      },
    };
  }
}

export class KVLeaseManager implements LeaseManager {
  async acquire(
    runId: RunId,
    owner: WorkerId,
    ttlMs: number
  ): Promise<LeaseHandle | null> {
    void runId;
    void owner;
    void ttlMs;
    throw new Error("Not implemented");
  }
}

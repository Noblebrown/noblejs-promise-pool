import { JobMetadata } from "../types/config";

/**
 * A generic error thrown by the pool. May contain {@link JobMetadata} if present and the original error thrown if present, ie,
 * the job throwns an error in the user-submitted promise, which is caught and wrapped in the `PoolError` along with the `JobMetadata` for that job.
 */
export class PoolError<R> extends Error {
  constructor(message?: string, public readonly jobMetadata?: JobMetadata<R>, originalError?: unknown) {
    super(message, originalError !== undefined ? { cause: originalError } : undefined);
    this.name = 'PoolError';
  }
}

/**
 * An error specifically thrown during job execution when the job is aborted.
 */
export class PoolAbortError<R> extends Error {
  public readonly poolInvoked = true;
  constructor(message?: string, public readonly jobMetadata?: JobMetadata<R>) {
    super(message);
    this.name = 'PoolAbortError';
  }
}
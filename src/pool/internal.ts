import { AwaitDestinationInstruction, IQueueItem } from "../types/behavior";
import { ExecutionStats, JobMetadata, JobMetadataSerializer, PoolStatus, SubmittedPromise } from "../types/config";
import { Logger, LoggerFunc, LogLevel } from "../types/logger";
import { PoolError } from "./errors";

/**
 * Wrapper for the user-submitted logger so we can set log functions based on {@link LogLevel} and leave the rest undefined
 * rather than make unnecessary calls to a stub.
 */
export class WrappedLogger<R> {
  constructor(public readonly level: LogLevel, public readonly serializer?: JobMetadataSerializer<R>, logger?: Partial<Logger>) {
    if (logger){
      //This is a bit janky, but should be enforced before we even get here, so all we're doing is making Typescript accept what we already know is type checked.
      if (level >= LogLevel.ERROR && logger.error) {
        this.error = this.createLoggerFunc(logger.error!);
        if (level >= LogLevel.WARN && logger.warn) {
          this.warn = this.createLoggerFunc(logger.warn!);
          if (level >= LogLevel.INFO) {
            this.info = this.createLoggerFunc(logger.info!);
            if (level >= LogLevel.DEBUG) {
              this.debug = this.createLoggerFunc(logger.debug!);
            }
          }
        }
      }
    }
  }

  public error?: (message?: unknown, metadata?: JobMetadata<R>, ...optionalParams: unknown[]) => void;
  public warn?: (message?: unknown, metadata?: JobMetadata<R>, ...optionalParams: unknown[]) => void;
  public debug?: (message?: unknown, metadata?: JobMetadata<R>, ...optionalParams: unknown[]) => void;
  public info?: (message?: unknown, metadata?: JobMetadata<R>, ...optionalParams: unknown[]) => void;

  private createLoggerFunc = (loggerFunc: LoggerFunc): (message?: unknown, metadata?: JobMetadata<R>, ...optionalParams: unknown[]) => void => {
    return (message?: unknown, metadata?: JobMetadata<R>, ...optionalParams: unknown[]) => {
      try {
        if (metadata) {
          loggerFunc(message, ...[(this.serializer ? this.serializer(metadata) : JSON.stringify(metadata)), ...optionalParams]);
        } else {
          loggerFunc(message, ...optionalParams);
        }
      } catch {
        //Just continue, we can't exactly log it.
      }
    }
  }
}

export type InternalSubmission<T, R, D> = {
  instruction: AwaitDestinationInstruction;
  fallbackInstruction?: AwaitDestinationInstruction;
  submittedPromise: SubmittedPromise<T, R, D>;
  queueInsertionTime?: D;
  submissionTime?: D,
  wakeupTimer?: ReturnType<typeof setTimeout>;
  id?: number;
  runId?: number;
  executionStats: ExecutionStats;
  type: InternalSubmissionType
  source: InternalSubmissionSource;
  resubmitDuringAwaiting?: boolean;
}

export type InternalSubmissionType = 'new' | 'retry' | 'recurrence' | 'none';
export type InternalSubmissionSource = 'new' | 'pool' | 'queue';

export type WrappedResultConfig<T> = {
  result?: T;
  error?: unknown;
  timedOut?: boolean;
  errorMessage?: string;
  abortedByPool?: boolean;
  abortReason?: unknown;
  status: 'success' | 'error' | 'halted';
}

export interface InternalQueueItem<T, R, D> extends IQueueItem<R> {
  readonly submission: InternalSubmission<T, R, D>;
  readonly errors?: PoolError<R>[];
  init: () => void;
}

export type PoolState = {
  poolSize: number,
  poolIdCounter: number,
  eventIdCounter: number,
  status: PoolStatus
};

export type DeferredQueueItem<T, R, D> = {
  submission: InternalSubmission<T, R, D>;
  insertionTime: D;
  /**
   * Use this in the await state if you want the time a submission spends in the deferred queue to count against its delay. Defaults to `false`.
   */
  countDeferredAgainstDelay: boolean;
};
import type { PoolError } from "../pool/errors";
import type { DeferredQueueItem, WrappedLogger } from "../pool/internal";
import type { AwaitDestinationInstruction, AwaitingStatus, AwaitResultBehaviorFunc, DeferredQueueFunc, DelayType, DestinationInstruction, HandlerFunctions, OnQueueStallFunc, OnResultFunc, PoolEvent, PoolHandlerFunctions, QueueBehaviorFunc, ResultBehaviorFunc, ReturnBehaviorFunc, SubmissionBehaviorFunc, TimestampProvider } from "./behavior";
import type { DebugLogger, ErrorLogger, InfoLogger, LogLevel, WarnLogger } from "./logger";

/**
 * Configuration specifically for the await state. NOTE: If you are going to implement your own success/error behavior functions, then be aware of the
 * `deferred_queue` destination option if you want the job to be resubmitted after the await state completes!
 */
export type AwaitConfig<T, R, D> = {
  /** 
   * Maximum recurrences after the await state is triggered. This serves as a possible lower cap than the job/global max recurrences when using the default
   * behavior. Defaults to 0.
   */
  maxRecurrences?: number;
  /** 
   * Maximum retries after the await state is triggered. This serves as a possible lower cap than the job/global max retries when using the default behavior.
   * Defaults to 0.
   */
  maxRetries?: number;
  /** 
   * {@link AwaitResultBehaviorFuncBehavior} Behavior function to determine what to do on error. Defaults to `behavior.DefaultAwaitErrorBehavior`, 
   * which will place any jobs that have the reached the `AwaitConfig.maxRetries` (but NOT the job or global max retries) into the `deferred_queue`.
   * If the job/global max retries is reached first then the job will be discarded as usual unless explicitly configured otherwise.
   */
  errorBehavior?: AwaitResultBehaviorFunc<T, R, D, AwaitDestinationInstruction>;
  /** 
   * {@link AwaitResultBehaviorFuncBehavior} Behavior function to determine what to do on success. Defaults to `behavior.DefaultAwaitSuccessBehavior`, 
   * which will place any jobs that have reached the `AwaitConfig.maxRetries` (but NOT the job or global max retries) into the `deferred_queue`.
   * If the job/global max retries is reached first then the job will be discarded as usual unless explicitly configured otherwise.
   */
  successBehavior?: AwaitResultBehaviorFunc<T, R, D>;
  /**
   * Provided for a possible edge case where specific queue behavior could skip non-delayed items in the queue and the pool is empty. This case could
   * lead to a constant cycle and stall. Use this if that's possible with your queue behavior. Default behavior simply returns `abort` in this state.
   * @returns `throw` if this state should throw an error, `abort` if it should abort the await state, and `wait` if it should wait for a specified
   * duration and try again. Be careful with `wait` as you could get stuck in a loop of waiting if the queue is only non-delayed items andyour queue 
   * behavior function never returns one of them!
   */
  onQueueStall?: OnQueueStallFunc;
  /**
   * A function to determine how to drain the deferred queue back into the pool once the awaiting state completes. By default this will simply attempt
   * to resubmit every item in the deferred queue back into the pool in order while also taking `countDeferredAgainstDelay` into account if the item
   * has a delay. Pass your own function if you need more detailed handling on how and in what order items from the deferred queue should be resubmitted
   * back into the pool. Items will be resubmitted in the order in which they appear in the returned array.
   */
  deferredQueueBehavior?: DeferredQueueFunc<T,R,D>;
};

export type DeferredQueueInstruction<T,R,D> = {
  item: DeferredQueueItem<T,R,D>,
  instruction: DestinationInstruction;
};

/** Configuration for how many times a job should retry on error*/
export type RetryConfig = {
  retries: number;
};

/** Configuration for how many times a job should recur on success*/
export type RecurrenceConfig = {
  recurrences: number;
}

/** Global configuration for the PromisePool */
export type PromisePoolConfig<T, R, D> = {
  /** Handler functions to plug in various behaviors */
  handlerFunctions?: PoolHandlerFunctions<R> & HandlerFunctions<R, D>;
  /** The number of available slots in the pool */
  poolSize: number;
  /** Optional logger configuration */
  logger?: UserLogger<R>;
} & SharedConfig<T, R, D> & TimestampConfig<D>;

export type BehaviorFunctions<T = void, R = unknown, D = number> = {
  /** 
   * {@link OnResultFunc} A function to determine if a job was successful or a failure. Defaults to `behavior.DefaultOnResultBehavior`, 
   * which returns an error if the job promise was rejected and success if it was resolved. This function can optionally be async and return a Promise,
   * which will be awaited. If an error is thrown then the job will be considered in error and treated accordingly.
   */
  onResultBehavior?: OnResultFunc<T, R>,
  /**
   * {@link ReturnBehaviorFunc} Determines if the job should return the `PoolResult` or not. Defaults to `behavior.ReturnBehavior.RETURN`, which will return a value if the `context.result.result` field exists.
   */
  returnBehavior?: ReturnBehaviorFunc<T, R, D>;
  /**
   * {@link QueueBehaviorFunc} Determines the behavior of the queue and which item will be selected next when a pool slot opens up. Defaults to `behavior.QueueBehavior.FIFO`, which
   * will select items on a first in first out basis unless the item is explicitly delayed in the queue.
   */
  queueBehavior?: QueueBehaviorFunc<T, R, D>;
  /**
   * {@link ResultBehaviorFunc} What to do if the job result is an error. Defaults to `behavior.ErrorBehavior.CONTINUE`, which will resubmit jobs that have not reached `maxRetries` and will
   * simply continue to run other jobs and never halt the pool.
   */
  errorBehavior?: ResultBehaviorFunc<T, R, D, DestinationInstruction>;
  /**
   * {@link ResultBehaviorFunc} What to do if the job result is a success. Defaults to `behavior.DefaultSuccessBehavior`, which will rerun the job if it has not
   * reached `maxRecurrences` and will otherwise continue to run other jobs.
   */
  successBehavior?: ResultBehaviorFunc<T, R, D, DestinationInstruction>;
  /**
   * {@link SubmissionBehaviorFunc} What to do when a job is first submitted to the pool. Defaults to `behavior.DefaultSubmissionBehavior`,
   * which will place it into the pool if possible, and will place it at the back of the queue if not.
   */
  submissionBehavior?: SubmissionBehaviorFunc<T, R, D>;
}

/** A wrapper around a running job within the pool */
export interface IPoolPromise<R> {
  /** 
   * This will adjust the delay on a job that is delayed in the pool. The value passed will replace the existing delay and be the new delay starting now.
   * This will have no effect on a job that is not in a delayed state.
   */
  adjustDelay: (newDelay: number) => number;
  getRemainingDelay: () => number;
  getStatus: () => PromiseStatus;
  /**
   * This will cancel a running job in the pool.
   * @param hardStop If this should be a hard stop with zero postprocessor or anything else other than standard cleanup. Otherwise the various callbacks will still run.
   * @param reason A string message explaining why this job was stopped, largely for informational/logging purposes.
   * @returns 
   */
  cancel: (hardStop?: boolean, reason?: string) => number;
  getJobMetadata: () => JobMetadata<R> | undefined;
  readonly id: number;
}

/**
 * The status of a `IQueueItem` in the queue.
 * 
 * `waiting` - The item is ready to be picked up and placed in the pool.
 * 
 * `delayed` - The item is currently in a delayed state and may not be eligible to be placed in the pool until the delay completes.
 * 
 * `canceled` - The item has been canceled and should not be placed in the pool as it will be removed shortly.
 */
export type QueueItemStatus = 'waiting' | 'delayed' | 'canceled';

/**
 * Where a job should be placed on submission.
 * 
 * `pool` - Submit this job to the pool. Note that if there is no room in the pool or if this is a retry or recurrence, 
 * default behavior will place this job at the back of the queue.
 * 
 * `force_pool` - Put this into the pool if at all possible. Use this for retries or recurrences that need to go directly back into the pool regardless
 * of any items in the queue. If it is not possible to place this job in the pool, it will be placed at the front of the queue.
 * 
 * `front_of_queue` - Put the job at the front of the queue.
 * 
 * `back_of_queue` - Put the job at the back of the queue.
 * 
 * `queue_index` - Place this job into a specified index in the queue.
 * 
 * `none` - Discard the job entirely and continue.
 */
export type JobDestination = 'pool' | 'force_pool' | 'front_of_queue' | 'back_of_queue' | 'queue_index' | 'none';

/**
 * Used for the awaiting state. This contains everything in {@link JobDestination} with one addition.
 * 
 * `deferred_queue` - Place this item into the deferred queue. It will stay there until `awaitResults()` completes, after which it will be processed into the queue or the pool.
 */
export type FullJobDestination = 'deferred_queue' | JobDestination;

/**
 * A wrapper around a job result containing various metrics and other information in addition to the actual result.
 */
export type WrappedResult<T, R, D> = BaseResult<T, R, D> & HaltedConditional<T, R>;

export type BaseResult<T, R, D> = {
  readonly id: number;
  readonly runId: number;
  readonly metrics: Metrics<R, D>;
  readonly submittedPromise: SubmittedPromise<T, R, D>;
  readonly currentDelay?: number;
  readonly executionStats: Readonly<ExecutionStats>;
  readonly abortReason?: unknown;
  readonly abortedByPool?: boolean;
  readonly timedOut?: boolean;
};

/** Execution statistics for a job */
export type ExecutionStats = {
  /** The total number of times this job has been retried. Initial execution is not included. */
  totalRetries: number;
  /** The number of times this job has been consecutively retried. */
  consecutiveRetries: number;
  /** The total number of times that this job has been executed. */
  totalExecutions: number;
}

/** 
 * The status of a job in the pool
 * 
 * `halted` - Execution of the job has been halted, generally a hard stop.
 * 
 * `delayed` - This job is delayed in the pool and will run once the delay is complete.
 * 
 * `running` - The job is currently running.
 * 
 * `error` - The job is in an error state and will be processed accordingly.
 * 
 * `callback` - The job has completed and is currently in the post-processing callback state.
 * 
 * `complete` - The job is done and will be removed from the pool shortly.
 */
export type PromiseStatus = NonHaltedStatus | HaltedStatus;

/** A collection of metrics for an individual job. */
export type Metrics<R, D> = {
  readonly error?: Error;
  readonly jobMetadata?: JobMetadata<R>;
  readonly id: number;
  readonly runId: number;
  /** The timestamp at which this job was submitted */
  readonly submissionTime: D;
  /** The start time of the user-submitted promise execution */
  readonly executionStartTime: D;
  /** The completion time of the user-submitted promise. This could still be present for halted/aborted executions. */
  readonly executionCompleteTime: D;
  /** The queue insertion time, if present */
  readonly queueInsertionTime?: D;
};

export type FullMetrics<R,D> = Metrics<R,D> & {
  /** Total time from end to end, from queue to completion of callbacks. */
  readonly endTime: D;
}

/** 
 * An optional metadata class if you need to attach that for identifying the result of specific jobs. None of this is used directly by the pool itself
 * aside from logging with a `JobMetadataSerializer`.
 */
export type JobMetadata<R> = {
  /** A user-provided ID for a specific job. */
  jobId?: string | symbol | number;
  /** A user-provided group ID for a specific job. */
  groupId?: string | symbol | number;
  /** An array of user-provided tags for a specific job. */
  tags?: (string | symbol | number)[];
  /** Used with type `R` if you need further metadata. This can be whatever type you want it to be.*/
  additionalData?: R;
}

/**
 * Wrapper around results gathered while in the await state.
 */
export type AwaitResults<T, R> = {
  /** An array of {@link PoolResult} returned from each job */
  results: PoolResult<T, R>[];
  /** The final state of the await process */
  status: AwaitingStatus;
  /** A string reason for logging if the await state was terminated in some way. */
  reason?: string;
  /** An error if the await state ended with an `error` status. */
  error?: unknown;
}

/**
 * An optional serializer for displaying {@link JobMetadata} in logs. Defaults to `JSON.stringify()`.
 */
export type JobMetadataSerializer<R> = (metadata: JobMetadata<R>) => string;

/**
 * Behavioral options when changing the status of the pool.
 */ 
export type PoolStatusOptions<T, R, D> = {
  /** 
   * If the pool is being set to `stopped` then this determines how the pool can shut down. Defaults to `immediate`, which will halt all jobs outright. 
   * This will always be treated as `immediate` if the pool is being set to `halted`.
   */
  closeType?: PoolCloseType,
  /** An {@link EventSilenceConfig} that will apply specifically to events triggered by the state change */
  eventSilenceConfig?: PartialEventSilenceConfig<T, R>,
  /** An optional string message for logging purposes. */
  message?: string,
  /** A {@link QueueBehaviorFunc} specifically if the queue is to be processed for this state change. */
  queueBehaviorFunction?: QueueBehaviorFunc<T, R, D>
}
/**
 * Internal wrapper around a job submitted by the user.
 */
export type SubmittedPromise<T, R, D> = Required<SharedConfig<T, R, D, true>> & BaseSubmission<T, R> & InternalLogger<R> & { timestampProvider: TimestampProvider<D> };

/**
 * Wrapper around the job submitted directly by the user.
 */
export type UserSubmittedPromise<T, R, D> = BaseSubmission<T, R> & SharedConfig<T, R, D> & { 
  //Optional because this must be set on the pool if D != number, but there's no requirement to set it manually on each promise.
  timestampProvider?: TimestampProvider<D>;
  logger?: UserLogger<R>;
};
/**
 * Modes for silencing or not silencing events based on inheritance. This does not apply to specific internal pool events. If there is a hard conflict,
 * ie then the child wins as it's closer to the individual job.
 * 
 * `silent_with_inherit` - The event will not fire unless overridden by `force_active`
 * 
 * `active_with_inherit` - The event will fire unless overridden by `force_silent`
 * 
 * `force_silence` - The event will not fire unless a child is set to `force_active`
 * 
 * `force_active` - The event will always fire unless a child is set to `force_silent`
 * 
 * Examples:
 * 
 * pool.event = `force_silence`, job.event = `force_active`, result = event will fire
 * 
 * pool.event = `force_active`, job.event = `force_silence`, result = event will not fire.
 * 
 * pool.event = `force_silence`, job.event = `active_with_inherit`, result = event will not fire.
 * 
 * pool.event = `silent_with_inherit`, job.event = `active_with_inherit`, result = event will not fire.
 */
export type SilenceMode = 'silent_with_inherit' | 'active_with_inherit' | 'force_silence' | 'force_active';

/**
 * A configuration for determine which events will fire and how. Use this on the global pool config to configure base behavior. Use it on individual jobs
 * if you want to override that base behavior to fire or not fire specific events. All events will default to `active_with_inherit` if not set.
 */
export type FullEventSilenceConfig<T, R> = Record<keyof PoolEvent<T, R>, SilenceMode>;

export type PartialEventSilenceConfig<T, R> = Partial<Record<keyof PoolEvent<T, R>, SilenceMode>>;

/**
 * These values determine how the pool should behave when the state changes.
 * 
 * `continue` - Keep everything running as usual.
 * 
 * `clear_queue` - Clear the queue, but allow jobs in the pool to complete normally.
 * 
 * `finish_callbacks` - Clear the queue and pool of any jobs that have not finished running the user-submitted promise. Allow others to complete their callbacks.
 * 
 * `immediate` - Shut down everything regardless of state. Clears both the pool and queue.
 */
export type PoolCloseType = 'continue' | 'clear_queue' | 'finish_callbacks' | 'immediate';

/**
 * The current state of the pool.
 * 
 * `running` - The pool is running normally.
 * 
 * `closed` - The pool is finishing work, but cannot accept any new job submissions.
 * 
 * `paused` - Jobs in the pool will complete, but no new items will be consumed from the queue. New jobs can be submitted, but will wait in the queue until
 * the state changes.
 * 
 * `stopped` - The pool will finish work, but will not accept new submissions and cannot change state. This is a terminal state.
 * 
 * `halted` - The pool has been hard stopped due to an error. It cannot be restarted and, depending on configuation, may not complete any jobs within.
 */
export type PoolStatus = 'running' | 'closed' | 'paused' | 'stopped' | 'halted';

/**
 * A collection of overall metrics about the pool itself.
 */
export type PoolMetrics = {
  runningJobs: number;
  queueSize: number;
  succeeded: number;
  failed: number;
}

//Configuration for the timestamp. The conditional ensures that if you use the `D` type in the PromisePool then you must provide a TimestampProvider.
export type TimestampConfig<D> = [D] extends [number] ? { timestampProvider?: undefined } : { timestampProvider: TimestampProvider<D> };

/**
 * A wrapper around the result from a job that can be either stored by the pool or fired off in an onComplete callback.
 */
export type PoolResult<T, R> = {
  /** The {@link JobMetadata} from the job, if present. */
  jobMetadata?: JobMetadata<R>;
  /** The pool-assigned ID */
  id: number;
  /** The results for all executions of this job */
  results: ValueOrError<T,R>[];
}

export type ValueOrError<T,R> = {
  /** The ID (1-indexed) for this specific execution */
  runId: number;
  /** The return value of the job, if present */
  value?: T,
  /** The error returned from the job, if present */
  error?: PoolError<R>;
  /** The final status of the job. */
  status: 'success' | 'error' | 'halted';
}

/** The result of a submission to the pool. */
export type SubmissionResult<R> = {
  /**
   * The destination of the submitted job. This may be different than the intended destination, ie, it was intended to be submitted to the pool, but the pool
   * was full and it went to the back of the queue instead.
   */
  destination: FullJobDestination;
  /**
   * The pool-assigned ID number for this job.
   */
  id?: number;
  /**
   * The {@link JobMetadata} that was passed with this submission, if present.
   */
  jobMetadata?: JobMetadata<R>;
}

/**
 * Indicates if the settled user-submitted promise should be treated as a success or failure.
 */
export type ResultOrError<T> = {
  /** The result from the user-submitted promise, if any. */
  result?: T;
  /** The value from the rejection of the user-submitted promise, if any. */
  error?: unknown;
  /** 
   * Whether this result should be treated as a success or error and which field we should use in turn.
   * 
   * `success` - The job completed successfully and the `result` field will be used, if present.
   * 
   * `error` - The job did not complete successfully and the `error` field will be used, if any.
   */
  status: 'success' | 'error';
}

/** Internal config object shared between multiple configs */
type SharedConfig<T, R, D, B = false> = {
  /** Specific handler functions */
  handlerFunctions?: HandlerFunctions<R, D>;
  /** A timeout in ms for jobs, either global or job-specific. If this timeout is exceeded then the job will be halted with the abort controller (if present) and will be treated as an error. */
  timeout?: number;
  /** Configuration for how this job should be resubmitted if it fails. */
  maxRetries?: number;
  /** Configuration for how this job should be resubmitted if it succeeds. */
  maxRecurrences?: number;
  /** Functions that dictate various pool behaviors. */
  behaviorFunctions?: B extends false ? BehaviorFunctions<T, R, D> : Required<BehaviorFunctions<T, R, D>>;
  /** If the pool should store metrics. The `handlerFunctions.metricsHandler` function will still fire, if present. Defaults to `false`.*/
  collectMetrics?: boolean;
  /** Configuration for silencing specific events */
  eventSilenceConfig?: PartialEventSilenceConfig<T, R>;
  /** Delay time before it executes or becomes eligible to enter the pool if queued. Values of 0 or less will be ignored. */
  delay?: number;
  /** 
   * The type of delay that designates this as strictly delayed once in the pool, a cumulative delay between pool and queue, or strictly a delay in the queue
   * before it becomes eligible to enter the pool. Defaults to `cumulateive` if there is a `delay` but no specified `delayType`.
   */
  delayType?: DelayType;
};

type LoggerSerializer<R> = {
  /** An optional serializer for {@link JobMetadata}. This is used for writing the metadata in logs. Defaults to `JSON.stringify()`. */
  jobMetadataSerializer?: JobMetadataSerializer<R>;
}

export type UserLogger<R> = LoggerSerializer<R> & ({
  /** Minimum log level for a logger. */
  logLevel: LogLevel.ERROR;
  /** An optional logger. The logger conforms to `console`, which can be passed if desired. */
  logger: ErrorLogger;
} | {
  /** Minimum log level for a logger. */
  logLevel: LogLevel.WARN;
  /** An optional logger. The logger conforms to `console`, which can be passed if desired. */
  logger: WarnLogger;
} | {
  /** Minimum log level for a logger. */
  logLevel: LogLevel.INFO;
  /** An optional logger. The logger conforms to `console`, which can be passed if desired. */
  logger: InfoLogger;
} | {
  /** Minimum log level for a logger. */
  logLevel: LogLevel.DEBUG;
  /** An optional logger. The logger conforms to `console`, which can be passed if desired. */
  logger: DebugLogger;
});

type HaltedConditional<T, R> = {
  readonly status: NonHaltedStatus;
  readonly result: PoolResult<T, R>;
} | {
  readonly status: HaltedStatus,
  readonly result?: PoolResult<T, R>;
}

type HaltedStatus = 'halted';
type NonHaltedStatus = 'delayed' | 'running' | 'callback' | 'complete' | 'error' ;

/** Logger used internally */
type InternalLogger<R> = {
  logger: WrappedLogger<R>
}

type BaseSubmission<T, R> = {
  /** The job function that will be executed to run the job. */
  promise: () => Promise<T>;
  /** Optional {@link JobMetadata} */
  jobMetadata?: JobMetadata<R>;
  /** 
   * An abort controller to cancel the current job. This is not required, but is recommended if you ever want to stop a running job. It is
   * recommended that you do not invoke this abort controller directly yourself, but use the pool to do so instead, ie, use `PromisePool.iterateRunningJobs()`
   * to find the job and use `IPoolPromise.cancel()` to kill it.
   */
  abortController?: AbortController;
  /**
   * An optional priority value if this item goes into the queue. Default behavior will preserve this value across retries/recurrences.
   */
  priority?: number;
}
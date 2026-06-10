import type { AwaitExecutionStats } from "../pool/await-execution-stats";
import { AwaitContext } from "../pool/contexts";
import type { PoolError } from "../pool/errors";
import { DeferredQueueItem } from "../pool/internal";
import type { PromisePool } from "../pool/promise-pool";
import type { DeferredQueueInstruction, ExecutionStats, FullJobDestination, JobDestination, JobMetadata, Metrics, PoolResult, PoolStatus, PoolStatusOptions, QueueItemStatus, ResultOrError, SubmittedPromise, WrappedResult } from "./config";

/**
 * Shared handler functions for the pool
 */
export type HandlerFunctions<R, D> = {
  /**
   * Optional function to dermine what happens with the metrics for a job when it completes. This does not intersect with the `collectMetrics` config,
   * but serves as more of an emitter for metrics when a job completes.
   */
  metricsHandler?: MetricsHandlerFunc<R, D>;
}

/**
 * Handlers for pool behavior
 */
export type PoolHandlerFunctions<R> = {
  /**
   * Optional function for extra functionality if the pool is halted
   */
  onPoolHalt?: OnPoolHaltFunc<R>;
}

/**
 * A function that takes a result or error and returns a `ResultOrError` object (or a promise for one) that tells the pool if the completed job should
 * be treated as a success or an error.
 */
export type OnResultFunc<T, R> = (result?: T, error?: unknown, jobMetadata?: JobMetadata<R>) => Promise<ResultOrError<T>> | ResultOrError<T>;
/**
 * A function to perform actions if ever the pool is put into a `halted` state.
 */
export type OnPoolHaltFunc<R> = (error: PoolError<R>) => void | Promise<void>;
/**
 * A handler for pool metrics. This operates independently of the `collectMetrics` flag and can function as an alternative to emit metrics if you need them
 * in (almost) real time. Metrics will be emitted within a `setImmediate()` block, so the call may be slightly delayed once the job is finished.
 */
export type MetricsHandlerFunc<R, D> = (metrics: Metrics<R, D>) => void | Promise<void>;
/**
 * A function that determines queue behavior and which item from the queue will be put into the pool next.
 */
export type QueueBehaviorFunc<T, R, D> = (context: IQueueContext<T, R, D>) => QueueInstruction<R>;
/**
 * A function that determines how to handle the result of a job. This function signature is used for both `onSuccessBehavior` and `onErrorBehavior`
 * to determine what should be done with a success or failure.
 */
export type ResultBehaviorFunc<T, R, D, I extends DestinationInstruction = DestinationInstruction> = (context: IResultContext<T, R, D>) => SuccessOrFailInstruction<T, R, D, I>;
/**
 * A function to determine how to handle job results while `awaitResults()` is in progress.
 */
export type AwaitResultBehaviorFunc<T, R, D, I extends AwaitDestinationInstruction = AwaitDestinationInstruction> = (context: IAwaitContext<T, R, D>) => SuccessOrFailInstruction<T, R, D, I>;
/**
 * A function that determines if the result of a job should return the value of the job for retention or discard it.
 */
export type ReturnBehaviorFunc<T, R, D> = (context: IResultContext<T, R, D>) => boolean;
/**
 * A function that determines what should be done with new submissions to the pool.
 */
export type SubmissionBehaviorFunc<T, R, D> = (context: ISubmissionContext<T, R, D>) => DestinationInstructionWithFallback;
/**
 * A function specifically for an edge case during `awaitResults()` regarding an empty pool and a queue with non-delayed items that are not returned by the
 * queue behavior function.
 */
export type OnQueueStallFunc = () => QueueStallInstruction;
/**
 * A function that determines how the deferred queue should be drained once `awaitResults()` has completed.
 */
export type DeferredQueueFunc<T,R,D> = (context: IDeferredQueueContext<T,R,D>) => DeferredQueueInstruction<T, R, D>[];

/**
 * Instruction on what to do if the queue stalls if `awaitResults()` has been called, the pool is empty, there are only non-delayed items in the queue,
 * and the queue behavior function isn't returning them for some reason. While this is an edge case, it's worth considering. Three actions are available:
 * 
 * `throw` - Throw an error, which will be caught by the function and halt things immediately.
 * 
 * `abort` - A softer version of throw that will wrap things up without processing the errant queue items.
 * 
 * `wait` - Wait for a specified duration and try again. Be careful with this as you could end up in an infinite wait loop if you're not careful!
 *
 */
export type QueueStallInstruction = {
  /** The action to perform */
  action: 'throw' | 'abort';
  time?: number;
} | {
  action: 'wait',
  /** The amount of time to wait. This is only required and used if the `action` field is set to `wait`. */
  time: number;
}
/**
 * Premade functions that can be used to determine if a job result value should be retained or ignored
 */
export const ReturnBehavior = {
  /**
   * @returns `false` in all cases, so no return value will be retained
   */
  NO_RETURN: () => false,
  /**
   * @returns `true` in all cases, so a `value` or `error` will be retained.
   */
  RETURN: () => true
} as const;

/**
 * Base function for retry behavior, used by multiple other functions and can be reused by the user if needed
 * @param context {@link IResultContext}
 * @param retryCount The current number of retries that have been attempted for this job
 * @param haltOnFail If the pool should halt if the job has reached max retries
 * @returns A `SuccessOrFailInstruction`
 */
export const BaseRetryBehavior = <T, R, D>(context: IResultContext<T, R, D>, retryCount: number, haltOnFail: boolean): SuccessOrFailInstruction<T, R, D, DestinationInstruction> => {
  const maxRetries = context.result.submittedPromise.maxRetries;
  if ((maxRetries > 0 && retryCount > 0 && maxRetries <= retryCount) || !maxRetries) {
    if (haltOnFail) {
      return {
        status: 'halted',
        options: {
          closeType: 'immediate',
          message: 'Pool halted due to job failure'
        },
        destinationInstruction: {
          destination: 'none'
        }
      };
    }
    return { destinationInstruction: { destination: 'none' } };
  } else {
    return { 
      destinationInstruction: { 
        destination: maxRetries !== 0 ? 'pool' : 'none', 
        delay: context.result.submittedPromise.delay,
        delayType: context.result.submittedPromise.delayType,
        priority: context.result.submittedPromise.priority
      } 
    };
  }
}

/**
 * Several premade functions that dictate what to do if the job result is determined to be an error
 */
export const ErrorBehavior = {
  /**
   * @param context {@link IResultContext}
   * @returns A `SuccessOrFailInstruction` that will never halt the pool, but will not resubmit the job if it reaches `maxRetries`. If `maxRetries` is not reached
   * then the job will be resubmitted to the pool.
   */
  CONTINUE: <T, R, D>(context: IResultContext<T, R, D>): SuccessOrFailInstruction<T, R, D, DestinationInstruction> => {
    return BaseRetryBehavior(context, context.result?.executionStats.totalRetries ?? 0, false);
  },
  /**
   * @param context {@link IResultContext}
   * @returns A `SuccessOrFailInstruction` that will halt the pool only if consecutive retry attemps reach `maxRetries`. Any successful execution will reset
   * the consecutive retry count to 0. If `maxRetries` is not reached then the job will be resubmitted to the pool.
   */
  HALT_ON_CONSECUTIVE_RETRY_FAILURE: <T, R, D>(context: IResultContext<T, R, D>): SuccessOrFailInstruction<T, R, D, DestinationInstruction> => {
    return BaseRetryBehavior(context, context.result?.executionStats.consecutiveRetries ?? 0, true);
  },
  /**
   * @param context {@link IResultContext}
   * @returns A `SuccessOrFailInstruction` that will halt the pool if retry attemps reach `maxRetries`. If `maxRetries` is not reached
   * then the job will be resubmitted to the pool.
   */
  HALT_ON_CUMULATIVE_RETRY_FAILURE: <T, R, D>(context: IResultContext<T, R, D>): SuccessOrFailInstruction<T, R, D, DestinationInstruction> => {
    return BaseRetryBehavior(context, context.result?.executionStats.totalRetries ?? 0, true);
  }
} as const;

/**
 * Canned functions for determining which item should be selected from the queue and placed in the pool when a slot opens up
 */
export const QueueBehavior = {
  /**
   * Basic FIFO. The first job submitted will be the first selected unless it was deliberately delayed in the queue.
   * @param context {@link IResultContext}
   * @returns A `QueueInstruction` with the next item to be pulled from the queue
   */
  FIFO: <T, R, D>(context: IQueueContext<T, R, D>): QueueInstruction<R> => {
    let index = -1;
    context.promisePool.iterateQueue((item, i) => {
      if (item.getStatus() !== 'canceled' && (item.getStatus() !== 'delayed' || item.getDelayType() === 'cumulative')) {
        index = i;
        return true;
      }
    });
    return { index };
  },
  /**
   * Selects jobs based on the optional `priority` field. Only use this if it meets your needs and you are using the `priority` field.
   * @param context {@link IQueueContext}
   * @returns A `QueueInstruction` with the next item to be pulled from the queue
   */
  PRIORITY: <T, R, D>(context: IQueueContext<T, R, D>): QueueInstruction<R> => {
    let next: IQueueItem<R> | undefined = undefined;
    let index = -1;
    context.promisePool.iterateQueue((q, i) => {
      if (q.getStatus() !== 'canceled' && (q.getStatus() !== 'delayed' || q.getDelayType() === 'cumulative') && (!next || (next.priority ?? 0) < (q.priority ?? 0))) {
        next = q;
        index = i;
      }
    });
    return { index };
  }
} as const;


/**
 * Default function for determining if the result of the user-submitted promise is an error or not. If the promise rejects with an error of some kind,
 * then we return an error status. If not, we return the result as a success.
 * @param result The result of the user-submitted promise.
 * @param error The error from the user-submitted promise
 * @returns A {@link ResultOrError} object
 */
export function DefaultOnResultBehavior<T>(result?: T, error?: unknown): ResultOrError<T> {
  if (error) {
    return { status: 'error', error };
  }
  return { status: 'success', result };
}

/**
 * Default behavior function determining how this job should be submitted. Defaults to `pool`.
 * @param context The {@link ISubmissionContext} containing the submission data
 * @returns A {@link DestinationInstruction}
 */
export function DefaultSubmissionBehavior<T,R,D>(context: ISubmissionContext<T,R,D>): DestinationInstructionWithFallback {
  return {
    destination: 'pool',
    delay: context.submittedPromise.delay,
    delayType: context.submittedPromise.delayType,
    priority: context.submittedPromise.priority
  };
};

/**
 * Default function for error behavior as part of the {@link AwaitConfig}. Defaults to putting items in the deferred queue unless they have a `maxRetries`
 * value, in which case we'll keep them cycling until they either complete or hit maxRetries. NOTE: This is the separate `maxRetries` value in the `AwaitContext`,
 * not the `maxRetries` value on the submitted job or the global pool setting. If a job is going to be discarded from the base function then this will not 
 * change that.
 * @param context The {@link AwaitContext}
 * @returns A {@link SuccessOrFailInstruction}
 */
export function DefaultAwaitErrorBehavior<T, R, D>(context: AwaitContext<T, R, D>): SuccessOrFailInstruction<T, R, D, AwaitDestinationInstruction> {
  const baseRetry = context.result.submittedPromise.behaviorFunctions.errorBehavior(context) as SuccessOrFailInstruction<T, R, D, AwaitDestinationInstruction>;
  if (!baseRetry.destinationInstruction) {
    baseRetry.destinationInstruction = { destination: 'none' };
  } else if (baseRetry.destinationInstruction.destination !== 'none' && context.result.submittedPromise.maxRetries != 0 &&
    context.frozenAwaitStats.exceedsRetries(context)) {
    baseRetry.destinationInstruction.destination = 'deferred_queue';
  }
  return baseRetry;
}

/**
 * Default function for success behavior as part of the {@link AwaitConfig}. Defaults to putting items in the deferred queue unless they have a `maxRecurrences`
 * value, in which case we'll keep them cycling until they either complete or hit maxRecurrences. NOTE: This is the separate `maxRecurrences` value in the `AwaitContext`,
 * not the `maxRecurrences` value on the submitted job or the global pool setting. If a job is going to be discarded from the base function then this will not 
 * change that.
 * @param context The {@link AwaitContext}
 * @returns A {@link SuccessOrFailInstruction}
 */
export function DefaultAwaitSuccessBehavior<T, R, D>(context: AwaitContext<T, R, D>): SuccessOrFailInstruction<T, R, D, AwaitDestinationInstruction> {
  const baseSuccess = context.result.submittedPromise.behaviorFunctions.successBehavior(context) as SuccessOrFailInstruction<T, R, D, AwaitDestinationInstruction>;
  if (!baseSuccess.destinationInstruction) {
    baseSuccess.destinationInstruction = { destination: 'none' };
  } else if (baseSuccess.destinationInstruction.destination !== 'none' && context.result.submittedPromise.maxRecurrences != 0 &&
    context.frozenAwaitStats.exceedsRecurrences(context)) {
    baseSuccess.destinationInstruction.destination = 'deferred_queue';
  }
  return baseSuccess;
}

/**
 * Default resubmission behavior from the deferred queue. This will calculate the new delay if needed and simply attempt to resubmit every job
 * to the pool in order.
 * @param context The {@link IDeferredQueueContext}
 * @returns a destination instruction for the deferred queue item
 */
export function DefaultDeferredQueueBehavior<T,R,D>(context: IDeferredQueueContext<T,R,D>): DeferredQueueInstruction<T,R,D>[] {
  return context.deferredQueue.map((item) => {
    let delay = 0;
    if (item.submission.submittedPromise.delay){
      if (item.countDeferredAgainstDelay){
        const provider = item.submission.submittedPromise.timestampProvider;
        const diff = provider.toMs(provider.subtract(provider.now(), item.insertionTime));
        delay = item.submission.submittedPromise.delay > diff ? item.submission.submittedPromise.delay - diff : 0;
      } else {
        delay = item.submission.submittedPromise.delay;
      }
    }
    return {
      item,
      instruction: {
        destination: 'pool',
        delay,
        delayType: item.submission.submittedPromise.delayType
      }
    };
  });
}

/**
 * Default function for success behavior. If there are no `maxRecurrences` or the job has reached `maxRecurrences` then the job is effectively done. 
 * If there are `maxRecurrences` and they have not been reached then it will be resubmitted to the pool.
 * @param context A {@link ResultContext}
 * @returns A {@link SuccessOrFailInstruction}
 */
export const DefaultSuccessBehavior = <T, R, D>(context: IResultContext<T, R, D>): SuccessOrFailInstruction<T, R, D, DestinationInstruction> => {
  const executionStats = context.result.executionStats;
  if (context.result.submittedPromise.maxRecurrences) { // Use > rather than >= because first execution does not count as a recurrence
    if (executionStats.totalExecutions > 0 && executionStats.totalExecutions - (executionStats.totalRetries ?? 0) > context.result.submittedPromise.maxRecurrences) {
      return { destinationInstruction: { destination: 'none' } };
    }
    return { 
      destinationInstruction: { 
        destination: 'pool',
        delay: context.result.submittedPromise.delay,
        delayType: context.result.submittedPromise.delayType,
        priority: context.result.submittedPromise.priority
      } 
    };
  }
  return { destinationInstruction: { destination: 'none' } };
}
/**
 * An optional timestamp provider. Use this if you need something other than ms and/or epoch time for various metrics in the pool. 
 * The pool will default to standard javascript {@link Date} functions if this is not used.
 */
export interface TimestampProvider<D = number> {
  /**
   * This should be an equivalent to `Date.now()`. It does not have to literally be epoch time in milliseconds, but it should be easily convertible to it
   * in {@link toMs()}.
   */
  now(): D,
  /**
   * Add two values together, so num1 + num2.
   * @param num1 First number
   * @param num2 Second number
   */
  subtract(num1: D, num2: D): D;
  /**
   * A function to convert the given `D` value to milliseconds. This could be a small number, ie, 5ms, or it could be converting a value that represents
   * epoch time. Make sure this implementation can handle both!
   * @param num The value to be converted
   */
  toMs(num: D): number;
}

/**
 * A function for iterating through the pool or the queue. Return `true` to halt iteration, `false`/`undefined` to continue iteration.
 */
export type IteratorFunction<I> = (item: I, index: number) => boolean | void;


/**
 * An array of event names that can be used by event listeners
 */
export const eventTypes = ['job_start', 'job_success', 'job_error', 'pool_empty', 'pool_state_change'] as const;

/**
 * A key representing the available event names
 */
export type EventKey = typeof eventTypes[number];

/**
 * A function map for each event as they have different needs
 */
export type PoolEvent<T, R> = {
  [K in EventKey]:
  K extends 'job_start' ? (jobMetadata?: JobMetadata<R>) => void :
  K extends 'job_success' ? (result?: PoolResult<T,R> | undefined, jobMetadata?: JobMetadata<R>) => void :
  K extends 'job_error' ? (error: PoolError<R> | Error | undefined, jobMetadata?: JobMetadata<R>) => void :
  K extends 'pool_empty' ? () => void :
  K extends 'pool_state_change' ? (oldState: PoolStatus, newState: PoolStatus, reason?: unknown) => void :
  never
};

/**
 * Names of various states used while `PromisePool.awaitResults()` is running
 * 
 * `empty` - Returned if there's nothing to process in the pool or queue
 * 
 * `complete` - awaitResults() completed successfully
 * 
 * `halted` - The pool hit a halt state while processing awaitResults() and shut down. Results will be returned for the jobs that did complete.
 * 
 * `aborted` - The await state was cancelled. Results will be returned for the jobs that did complete.
 * 
 * `invalid` - The pool is not in a valid state for this operation. Invalid states are `halted`, `stopped`, and `paused`.
 * 
 * `error` - The awaitResults() function itself encountered an error and stopped prematurely. Results will be returned for the jobs that did complete.
 */
export type AwaitingStatus = 'empty' | 'complete' | 'halted' | 'aborted' | 'invalid' | 'error';

/**
 * Names of delay types that can be used when submitting jobs.
 * 
 * `pool` - Delay this strictly in the pool. It will not have a delay in the queue and will occuply a slot in the queue while delayed.
 * If the job cannot be placed in the pool then this will automatically be treated as `cumulative`.
 * 
 * `queue` - Delay this strictly in the queue. Default queue behavior functions will only add it to the pool or make it eligible once the delay fully completes.
 * 
 * `cumulative` - Delay in the queue, and if it is moved to the pool before the delay completes, then finish the delay in the pool.
 */
export type DelayType = 'pool' | 'queue' | 'cumulative';

/**
 * The value returned by the {@link QueueBehaviorFunc}
 */
export type QueueInstruction<R> = {
  value?: IQueueItem<R>;
  index?: number;
}

/**
 * Ensures that a queue_index must include an index, but anything else will ignore it.
 */
type DestinationConditional = {
  destination: Exclude<JobDestination, 'queue_index'>;
  index?: number;
} | {
  destination: 'queue_index';
  /** Only matters if `destination` is set to `queue_index` */
  index: number;
};

export type DestinationInstruction = BaseDestinationInstruction & DestinationConditional;

type BaseDestinationInstruction = {
  /** Optional priority to assign if this job is placed in the queue */
  priority?: number;
  /** 
   * A delay to put on the job if it's being resubmitted. NOTE: Explicitly set this as `0` if there is a global delay and you want this job to 
   * execute without a delay. Leaving it `undefined` will default to the global default.
   */
  delay?: number;
  /** The type of delay, whether you want it strictly delayed in the queue, the pool, or a combination of both. */
  delayType?: DelayType;
}

export type DestinationInstructionWithFallback = DestinationInstruction & {
  /**
   * A fallback in case the original instruction cannot be performed. There are specific defaults for this, but the fallback allows those defaults
   * to be overridden. If this has the same {@link JobDestination} as the `destinationInstruction` then it will be ignored and defaults will be used.
   * This will be coalesced with the primary instruction, so be sure to declare any fields you need to override, even if they're falsy values like `0`,
   * ie, the primary instruction has a `delay` of 10. If no delay is specified in the fallback, then it will inherit that `delay` of 10. If don't want
   * any delay on the fallback, give the fallback a `delay` value of `0`.
   * 
   * `force_pool` - If there is no room in the pool at all, then the default fallback is `front_of_queue`
   * `pool` - If this is a retry/recurrence or there is no room in the pool, then the default fallback is `back_of_queue`
   * `back_of_queue`,`front_of_queue`,`queue_index` - If this job does not have a queue-specific delay and there is room in the pool, then it
   *  will default to `pool`.
   */
  fallbackInstruction?: DestinationInstruction;
}

/**
 * Instructions for placing a submitted job into its proper destination, whether that be the queue or the pool, and how.
 */
export type AwaitDestinationInstruction = BaseDestinationInstruction & AwaitDestinationConditional & {
  /**
   * Use this if you want the time spent in the deferred queue to count against the delay.
   */
  countDeferredAgainstDelay?: boolean;
};

export type AwaitDestinationInstructionWithFallback = AwaitDestinationInstruction & {
  /**
   * A fallback in case the original instruction cannot be performed. There are specific defaults for this, but the fallback allows those defaults
   * to be overridden. If this has the same {@link JobDestination} as the `destinationInstruction` then it will be ignored and defaults will be used.
   * 
   * `force_pool` - If there is no room in the pool at all, then the default fallback is `front_of_queue`
   * `pool` - If this is a retry/recurrence or there is no room in the pool, then the default fallback is `back_of_queue`
   * `back_of_queue`,`front_of_queue`,`queue_index` - If this job does not have a queue-specific delay and there is room in the pool, then it
   *  will default to `pool`.
   */
  fallbackInstruction?: DestinationInstruction;
}

type AwaitDestinationConditional = {
  destination: Exclude<FullJobDestination, 'queue_index'>;
  index?: number;
} | {
  destination: 'queue_index';
  index: number;
};

/**
 * An instruction returned by several different functions to determine if a job completed successfully or failed, and instructions on what to do with the result.
 */
export type SuccessOrFailInstruction<T, R, D, Instruction extends AwaitDestinationInstruction = AwaitDestinationInstruction> = {
  /** Optional field if you want to change the pool state as a result of this job */
  status?: PoolStatus,
  /** An optional config for  */
  options?: PoolStatusOptions<T, R, D>,
  /** A {@link DestinationInstruction} object (or one that extends it) to detail where this job should go and how */
  destinationInstruction: Instruction;
}

export interface IBaseContext<T, R, D> {
  readonly promisePool: PromisePool<T, R, D>;
}

export interface ISubmissionContext<T, R, D> extends IBaseContext<T, R, D> {
  readonly submittedPromise: SubmittedPromise<T, R, D>;
}

export interface IResultContext<T, R, D> extends IBaseContext<T, R, D> {
  readonly result: WrappedResult<T, R, D>;
}

export interface IQueueContext<T, R, D> extends IBaseContext<T, R, D> {
  readonly result?: WrappedResult<T, R, D>;
}

export interface IDeferredQueueContext<T,R,D> extends IBaseContext<T,R,D> {
  readonly deferredQueue: DeferredQueueItem<T, R, D>[];
}

export interface IAwaitContext<T,R,D> extends IResultContext<T,R,D> {
  readonly frozenAwaitStats: AwaitExecutionStats<T, R, D>,
  readonly maxRecurrences: number,
  readonly maxRetries: number
}

/** Represents an item waiting in the queue. */
export interface IQueueItem<R> {
  /** The internally assigned ID of this item in the queue. This ID will be reused in the pool. */
  readonly id: number;
  /** An optional priority field if needed. */
  priority?: number;
  getStatus: () => QueueItemStatus;
  /** Clear the delay timer if present. */
  clearTimer: () => number;
  /** Alter the delay timer. This will set a new delay timer starting from the current time. */
  adjustDelay: (newDelay: number) => number;
  getRemainingDelay: () => number;
  /** Cancel this item so it will be removed from the queue. */
  cancel: () => number;
  getDelayType: () => DelayType | undefined;
  getJobMetadata: () => JobMetadata<R> | undefined;
}

/** Frozen {@link ExecutionStats} for each job in the queue and pool. This can be used to calculate retries/recurrences against the live stats on running jobs. */
export interface IAwaitExecutionStats<T,R,D> {
  /** Get the stats for the running job. Returns {@link ExecutionStats} or undefined if there are no stats for the given ID. */
  getStats(context: AwaitContext<T, R, D>): ExecutionStats | undefined;
  /** Check to see if the job in question has reached `maxRetries`. */
  exceedsRetries(context: AwaitContext<T, R, D>): boolean;
  /** Check to see if the job in question has reached `maxRecurrences` */
  exceedsRecurrences(context: AwaitContext<T, R, D>): boolean;
};
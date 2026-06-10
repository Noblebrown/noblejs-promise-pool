import { EventKey, PoolEvent } from "../types/behavior";
import { BaseResult, ExecutionStats, IPoolPromise, JobMetadata, PromiseStatus, ResultOrError, SilenceMode, WrappedResult } from "../types/config";
import { PoolAbortError, PoolError } from "./errors";
import { InternalSubmission, WrappedLogger, WrappedResultConfig } from "./internal";

/**
 * This class wraps the user-submitted job with various metrics and behavior. This class should not be exposed directly to the user.
 * Only expose the interface.
 */
export class PoolPromise<T, R, D> implements IPoolPromise<R> {
  public status: PromiseStatus;
  private executionStartTime!: D;
  private executionEndTime!: D;
  public runningPromise!: Promise<WrappedResult<T, R, D>>;
  public readonly executionStats: ExecutionStats; //Only used for awaitResults when building baseline stats
  private delayData: {
    delayTimeout?: ReturnType<typeof setTimeout>;
    delayAbortController?: AbortController;
    setNewTimeout?: (timeout: number) => void;
  } = {};
  private abortController?: InternalAbortController<R>;
  private timeoutAbortController?: AbortController;
  private poolPromiseAbortController = new AbortController();
  private fullDelay: number;
  private logger: WrappedLogger<R>;

  constructor(
    private readonly submission: InternalSubmission<T, R, D>,
    private readonly processResult: (jobResult: WrappedResult<T, R, D>) => WrappedResult<T, R, D>,
    public readonly id: number,
    private readonly runId: number,
    private readonly invokeListeners: <K extends EventKey>(event: K, mode: SilenceMode, payload: Parameters<PoolEvent<T, R>[K]>) => void,
    private readonly submitTime: D
  ) {
    this.logger = submission.submittedPromise.logger;
    this.executionStats = Object.freeze({ ...submission.executionStats }); //Copy and freeze so the original can't get manipulated by the user
    if (submission.submittedPromise.abortController) {
      this.abortController = new InternalAbortController(submission.submittedPromise.abortController, this.logger);
    }
    this.fullDelay = submission.instruction.delayType !== 'queue' ? (submission.instruction.delay ?? submission.submittedPromise.delay) : 0;
    this.status = this.fullDelay > 0 && (submission.instruction.delayType || submission.submittedPromise.delayType) !== 'queue' ? 'delayed' : 'running';
  }

  async execute(): Promise<void> {
    this.runningPromise = new Promise<WrappedResult<T, R, D>>(async (resolve) => {
      //Add an event listener to the user-provided abort controller if present since they could always invoke it directly themselves.
      this.abortController?.controller?.signal?.addEventListener('abort', () => {
        this.status = 'halted';
        resolve(this.createWrappedResult({
          status: 'halted',
          abortedByPool: false,
          abortReason: this.abortController!.controller.signal.reason
        }));
        this.timeoutAbortController?.abort();
        this.delayData.delayAbortController?.abort();
      }, { once: true });
      //We have to check this after every async operation as other things in the event loop could potentially mess with state
      let aborted: 'pool' | 'user' | undefined = undefined;
      try {
        //Overall abort controller if the job is cancelled via the API
        this.poolPromiseAbortController.signal.addEventListener('abort', () => {
          this.logger.debug?.('Abort controller invoked, halting', this.submission.submittedPromise.jobMetadata);
          resolve(this.createWrappedResult({
            status: 'halted',
            abortedByPool: true,
            abortReason: this.poolPromiseAbortController.signal.reason,
          }));
          this.delayData.delayAbortController?.abort();
          this.timeoutAbortController?.abort();
          if (this.status === 'running'){ //Don't invoke this if the promise hasn't run yet or has already completed.
            this.abortController?.abort(new PoolAbortError<R>('Aborted by pool', this.submission.submittedPromise.jobMetadata));
          }
          this.status = 'halted';
        }, { once: true });

        if (this.status === 'delayed') {
          this.delayData = {
            delayAbortController: new AbortController(),
          };
          this.logger.debug?.(`Delaying job for ${this.submission.instruction.delay}`, this.submission.submittedPromise.jobMetadata);
          await new Promise<void>((delayResolve) => {
            this.delayData.setNewTimeout = (timeout: number) => {
              clearTimeout(this.delayData.delayTimeout);
              this.delayData.delayTimeout = setTimeout(delayResolve, timeout);
            };
            this.delayData.setNewTimeout(this.fullDelay);
            this.delayData.delayAbortController!.signal.addEventListener('abort', () => {
              this.logger.debug?.(`Delay abort controller invoked`, this.submission.submittedPromise.jobMetadata);
              clearTimeout(this.delayData.delayTimeout);
              delayResolve(undefined);
            }, { once: true });
          }).finally(() => {
            this.delayData.delayAbortController = undefined;
          });
          aborted = this.isAborted();
          if (aborted) {
            if (aborted === 'user') {
              this.createAbortedResponse(resolve);
            }
            return;
          }
          this.status = 'running';
        }
      } catch (error) {
        aborted = this.isAborted();
        if (!aborted) { 
          this.status = 'error';
          this.logger.error?.('Error encountered during job delay in pool', this.submission.submittedPromise.jobMetadata);
          resolve(this.createWrappedResult({
            status: 'error',
            error,
            errorMessage: 'PoolPromise encountered an error during the delay',
            abortedByPool: this.abortController?.poolInvoked,
          }));
        } else if (aborted === 'user') { //If it's aborted then we don't really care about this error.
          this.createAbortedResponse(resolve);
          this.logger.info?.('Error encountered during job delay after being aborted', this.submission.submittedPromise.jobMetadata);
        }
        return;
      }

      let timedout = false;
      let result: T | undefined = undefined;
      let resultOrError: ResultOrError<T> | undefined = undefined;
      this.submission.executionStats.totalExecutions++;
      if (this.submission.type === 'retry') {
        this.submission.executionStats.consecutiveRetries = (this.submission.executionStats.consecutiveRetries ?? 0) + 1;
        this.submission.executionStats.totalRetries = (this.submission.executionStats.totalRetries ?? 0) + 1;
      }

      try {
        this.invokeListeners('job_start', this.submission.submittedPromise.eventSilenceConfig.job_start!, [this.submission.submittedPromise.jobMetadata]);
        if (this.submission.submittedPromise.timeout) {
          let raceSettled = false;
          this.timeoutAbortController = new AbortController();
          let timeoutPid: ReturnType<typeof setTimeout>;
          this.executionStartTime = this.submission.submittedPromise.timestampProvider.now();
          result = (await Promise.race([
            this.submission.submittedPromise.promise().catch((err: unknown) => {
              //We want to avoid this promise throwing an unhandled rejection if it rejects after losing the race, but don't want to swallow an exception that occurs before then.
              if (!timedout) {
                throw err;
              }
            }),
            new Promise<void>(
              (timeoutResolve, timeoutReject) => {
                this.timeoutAbortController!.signal.addEventListener('abort', () => {
                  clearTimeout(timeoutPid);
                  timeoutResolve();
                  this.logger.debug?.('Timeout abort controller was triggered', this.submission.submittedPromise.jobMetadata);
                }, { once: true });
                timeoutPid = setTimeout(() => {
                  if (!raceSettled) {
                    timedout = true;
                    const timeoutError = new PoolAbortError<R>(`Promise timed out after ${this.submission.submittedPromise.timeout} ms`, this.submission.submittedPromise.jobMetadata);
                    timeoutReject(timeoutError);
                    if (!this.abortController?.controller.signal.aborted) {
                      this.abortController?.abort(timeoutError);
                    }
                    this.logger.info?.(`Job failed to execute within the ${this.submission.submittedPromise.timeout}ms timeout`, this.submission.submittedPromise.jobMetadata);
                  }
                }, this.submission.submittedPromise.timeout);
              },
            ),
          ]).finally(() => {
            this.executionEndTime = this.submission.submittedPromise.timestampProvider.now();
            //Micro-edge case: User promise finishes a tiny fraction of a second before the timeout, and the timeout function is in the event loop when we get here.
            raceSettled = true;
            if (!timedout) {
              this.timeoutAbortController?.abort(); //Effectively a no-op if it's already been called, but clears the timeout otherwise
            }
            this.timeoutAbortController = undefined;
          })) as T;
        } else {
          this.executionStartTime = this.submission.submittedPromise.timestampProvider.now();
          result = await this.submission.submittedPromise.promise();
          this.executionEndTime = this.submission.submittedPromise.timestampProvider.now();
        }
        aborted = this.isAborted();
        if (!aborted) {
          this.logger.debug?.('Job resolved', this.submission.submittedPromise.jobMetadata);
          this.status = 'callback';
          resultOrError = await this.invokeOnResult(result);
        } else {
          if (aborted === 'user') {
            this.createAbortedResponse(resolve);
          }
          return;
        }
      } catch (err) {
        if (!this.executionEndTime){
          this.executionEndTime = this.submission.submittedPromise.timestampProvider.now();
        }
        aborted = this.isAborted();
        if (!aborted) {
          this.logger.debug?.('Job rejected', this.submission.submittedPromise.jobMetadata);
          this.status = 'callback';
          resultOrError = await this.invokeOnResult(result, err);
        } else {
          if (aborted === 'user') {
            this.createAbortedResponse(resolve);
          }
          return;
        }
      }
      aborted = this.isAborted();
      if (!aborted) {
        if (resultOrError.status === 'error') {
          this.status = 'error';
          resolve(this.createWrappedResult({
            error: resultOrError.error,
            result: resultOrError.result,
            status: resultOrError.status,
            errorMessage: "Error thrown from submitted promise",
            timedOut: timedout
          }));
          return;
        }
      } else {
        if (aborted === 'user') {
          this.createAbortedResponse(resolve);
        }
        return;
      }

      this.status = 'complete';
      resolve(this.createWrappedResult({
        error: resultOrError.error,
        status: resultOrError.status,
        result: resultOrError.result,
      }));
    }).then((result) => this.processResult(result));
  }

  private createAbortedResponse(resolve: (value: WrappedResult<T, R, D> | PromiseLike<WrappedResult<T, R, D>>) => void): void {
    this.status = 'halted';
    resolve(this.createWrappedResult({
      status: 'halted',
      abortedByPool: this.abortController?.poolInvoked,
      abortReason: this.poolPromiseAbortController.signal.aborted ? this.poolPromiseAbortController.signal.reason : this.abortController?.controller.signal.reason,
    }));
  }

  private isAborted(): 'pool' | 'user' | undefined {
    if (this.abortController?.controller?.signal?.aborted || this.poolPromiseAbortController.signal.aborted) {
      return this.abortController?.poolInvoked || this.poolPromiseAbortController.signal.aborted ? 'pool' : 'user';
    }
  }

  async invokeOnResult(result?: T, error?: unknown): Promise<ResultOrError<T>> {
    try {
      return await this.submission.submittedPromise.behaviorFunctions.onResultBehavior(result, error, this.submission.submittedPromise.jobMetadata);
    } catch (err: unknown) {
      return {
        status: 'error',
        error: err,
        result: result
      };
    }
  }

  private createWrappedResult(config: WrappedResultConfig<T>): WrappedResult<T, R, D> {
    let poolError: PoolError<R> | undefined = undefined;
    if (config.error) {
      poolError = new PoolError(config.errorMessage, this.submission.submittedPromise.jobMetadata, config.error);
    }
    const submittedPromise = this.submission.submittedPromise;
    const baseResult: BaseResult<T, R, D> = {
      id: this.id,
      runId: this.runId,
      submittedPromise,
      metrics: {
        id: this.id,
        runId: this.runId,
        submissionTime: this.submitTime,
        executionStartTime: this.executionStartTime,
        executionCompleteTime: this.executionEndTime,
        queueInsertionTime: this.submission.queueInsertionTime,
        jobMetadata: this.submission.submittedPromise.jobMetadata,
        ...(poolError ? { error: poolError } : {})
      },
      executionStats: { ...this.submission.executionStats },
      ...(config.abortedByPool ? { abortedByPool: config.abortedByPool } : {}),
      ...(config.abortReason ? { abortReason: config.abortReason } : {}),
      ...(config.timedOut ? { timedOut: config.timedOut } : {})
    };

    if (this.status !== 'halted') {
      return {
        ...baseResult,
        status: this.status,
        result: {
          jobMetadata: this.submission.submittedPromise.jobMetadata,
          id: this.id,
          results: [{
            runId: this.runId,
            status: config.status,
            ...(config.result ? { value: config.result } : {}),
            ...(poolError ? { error: poolError } : {}),
          }]
        }
      };
    } else {
      return {
        ...baseResult,
        status: this.status,
      };
    }
  }

  getStatus(): PromiseStatus {
    return this.status;
  }

  getRemainingDelay(): number {
    if (this.status === 'delayed') {
      return this.submission.submittedPromise.timestampProvider.toMs(this.submitTime) + this.fullDelay - Date.now();
    }
    return -1;
  }

  adjustDelay(newDelay: number): number { //newDelay = new total timeout, not an added extension of the existing delay
    if (this.status === 'delayed') {
      const remaining = this.getRemainingDelay();
      if (newDelay > 0) {
        this.fullDelay = newDelay + remaining;
        this.delayData.setNewTimeout!(newDelay);
      } else {
        this.delayData.delayAbortController?.abort();
      }
      return remaining;
    }
    return -1;
  }

  cancel(hardStop?: boolean, reason?: string): number {
    const remaining = this.adjustDelay(0);
    if (this.status === 'delayed') {
      this.delayData.delayAbortController?.abort();
      this.poolPromiseAbortController.abort();
    } else if (this.status === 'running') {
      this.poolPromiseAbortController.abort();
      this.abortController?.abort(new PoolAbortError<R>(reason, this.submission.submittedPromise.jobMetadata));
    } else if (hardStop && this.status === 'callback') {
      this.poolPromiseAbortController.abort();
    }
    this.status = 'halted';
    return remaining;
  }

  getJobMetadata(): JobMetadata<R> | undefined {
    return this.submission.submittedPromise.jobMetadata;
  }
}

class InternalAbortController<R> { //Simple convenience class that wraps the user-submitted abort controller so we can tell if the pool or the user triggered it
  controller: AbortController;
  poolInvoked: boolean;
  logger: WrappedLogger<R>;

  constructor(controller: AbortController, logger: WrappedLogger<R>) {
    this.controller = controller;
    this.poolInvoked = false;
    this.logger = logger;
    controller.signal.addEventListener('abort', () => {
      if (!this.poolInvoked) {
        this.logger.warn?.('User-provided abort controller was aborted outside of the pool. This is not recommended.');
      }
    }, { once: true });
  }

  abort(reason: PoolAbortError<R>) { //If we are invoking then we must always pass a PoolAbortError so the user knows what to look for
    this.poolInvoked = true;
    this.controller.abort(reason);
    this.logger.debug?.('User-provided abort controller was aborted by the pool', reason.jobMetadata);
  }
}




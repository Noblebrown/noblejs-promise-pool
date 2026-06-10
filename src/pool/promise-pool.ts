import { AwaitDestinationInstruction, AwaitDestinationInstructionWithFallback, AwaitingStatus, DefaultAwaitErrorBehavior, DefaultAwaitSuccessBehavior, DefaultDeferredQueueBehavior, DefaultOnResultBehavior, DefaultSubmissionBehavior, DefaultSuccessBehavior, DelayType, ErrorBehavior, EventKey, eventTypes, HandlerFunctions, IQueueContext, IQueueItem, IteratorFunction, PoolEvent, PoolHandlerFunctions, QueueBehavior, QueueBehaviorFunc, ReturnBehavior, SuccessOrFailInstruction, TimestampProvider } from "../types/behavior";
import { AwaitConfig, AwaitResults, BehaviorFunctions, ExecutionStats, FullEventSilenceConfig, FullJobDestination, FullMetrics, IPoolPromise, PartialEventSilenceConfig, PoolCloseType, PoolMetrics, PoolResult, PoolStatus, PoolStatusOptions, PromisePoolConfig, SilenceMode, SubmissionResult, SubmittedPromise, UserSubmittedPromise, WrappedResult } from "../types/config";
import { LogLevel } from "../types/logger";
import { AwaitExecutionStats } from "./await-execution-stats";
import { AwaitContext, DeferredQueueContext, QueueContext, ResultContext, SubmissionContext } from "./contexts";
import { PoolError } from "./errors";
import { DeferredQueueItem, InternalQueueItem, InternalSubmission, InternalSubmissionSource, PoolState, WrappedLogger } from "./internal";
import { PoolPromise } from "./pool-promise";
import { QueueItem } from "./queue-item";

/**
 * The core promise pool class. The possible types on the pool:
 * 
 * `T` - The optional return type of any jobs submitted the pool. Defaults to `void`.
 * 
 * `R` - The optional type for `additionalData` in the optional `JobMetadata`. Defaults to `unknown`.
 * 
 * `D` - The optional type for a `TimestampProvider` if you need timestamps other than standard `Date.now()`. Defaults to `number`.
 */
export class PromisePool<T = void, R = unknown, D = number> {
  private pool = new Map<number, PoolPromise<T,R,D>>();
  private queue: InternalQueueItem<T,R,D>[] = [];
  private poolState: PoolState = {
    poolSize: 10,
    poolIdCounter: 0,
    eventIdCounter: 0,
    status: 'running'
  };
  private deferredQueue: DeferredQueueItem<T,R,D>[] = [];
  private readonly noSubmissionStates = new Set<PoolStatus>(['closed','halted','stopped']);
  private readonly queueDestinations = new Set<FullJobDestination>(['front_of_queue', 'back_of_queue','queue_index']);
  private results: Map<number, PoolResult<T,R>> = new Map<number, PoolResult<T,R>>();
  private awaitingState: AwaitingState<T,R,D> = {
    awaitingResults: 'none'
  };

  private timestampProvider: TimestampProvider<D>;
  private handlerFunctions: HandlerFunctions<R,D> & PoolHandlerFunctions<R>;
  private behaviorFunctions: InternalBehaviorFunctions<T,R,D>;
  private retries: number;
  private recurrences: number;
  private timeout: number; //In ms
  private collectMetrics: boolean;
  private jobMetrics = new Map<number, FullMetrics<R,D>[]>();
  private delay: number;
  private delayType: DelayType;
  private poolMetrics = {
    failed: 0,
    succeeded: 0
  };
  private logger: WrappedLogger<R>;
  private eventSilenceConfig: FullEventSilenceConfig<T,R> = {
    job_error: "active_with_inherit",
    job_start: "active_with_inherit",
    job_success: "active_with_inherit",
    pool_empty: "active_with_inherit",
    pool_state_change: "active_with_inherit"
  };
  private listeners: {
    [K in keyof PoolEvent<T,R>]?: PoolListener<T,R,K>[]
  } = {};
  
  constructor(config: PromisePoolConfig<T,R,D>) {
    //It'd be nice if TS could enforce number ranges, but this will have to do
    if (config.poolSize <= 0) {
      throw new Error('The poolSize value must be greater than 0');
    }
    //Copy out the config so it can't be altered by the user outside of the pool. Assign defaults if no config present.
    this.retries = config.maxRetries ?? 0;
    this.recurrences = config.maxRecurrences ?? 0;
    this.poolState.status = 'running';
    this.poolState.poolSize = config.poolSize;
    this.timestampProvider = config.timestampProvider || DefaultTimestampProvider as TimestampProvider<D>;
    this.handlerFunctions = config.handlerFunctions || {};
    this.handlerFunctions.metricsHandler = config.handlerFunctions?.metricsHandler;
    this.handlerFunctions.onPoolHalt = config.handlerFunctions?.onPoolHalt;
    const behaviorFunctions: BehaviorFunctions<T,R,D> = {};
    behaviorFunctions.returnBehavior = config.behaviorFunctions?.returnBehavior || ReturnBehavior.RETURN;
    behaviorFunctions.submissionBehavior = config.behaviorFunctions?.submissionBehavior || DefaultSubmissionBehavior;
    behaviorFunctions.queueBehavior = config.behaviorFunctions?.queueBehavior || QueueBehavior.FIFO;
    behaviorFunctions.errorBehavior = config.behaviorFunctions?.errorBehavior || ErrorBehavior.CONTINUE;
    behaviorFunctions.successBehavior = config.behaviorFunctions?.successBehavior || DefaultSuccessBehavior;
    behaviorFunctions.onResultBehavior = config.behaviorFunctions?.onResultBehavior || DefaultOnResultBehavior;
    this.behaviorFunctions = behaviorFunctions as InternalBehaviorFunctions<T,R,D>;
    this.delay = config.delay ?? 0;
    this.delayType = config.delayType || 'cumulative';
    this.timeout = config.timeout ?? 0;
    this.collectMetrics = config.collectMetrics ?? false;
    this.logger = new WrappedLogger<R>(config.logger?.logLevel ?? LogLevel.NONE, config.logger?.jobMetadataSerializer, config.logger?.logger);
    if (config.eventSilenceConfig){
      this.eventSilenceConfig = {...this.eventSilenceConfig, ...config.eventSilenceConfig};
    }
    this.logger.debug?.('Pool created');
  }

  /**
   * Bulk batch submission
   * @param data An array of {@link UserSubmittedPromise}
   * @returns An array of {@link FullJobDestination} strings that indicate where each {@link UserSubmittedPromise} ended up.
   */
  submitBatch(data: UserSubmittedPromise<T,R,D>[]): SubmissionResult<R>[] {
    return data.map((d) => {
      return this.submit(d);
    });
  }

  /**
   * Submit a job to the pool
   * @param data A {@link UserSubmittedPromise} to be submitted to the pool
   * @returns A {@link FullJobDestination} string that indicates where the {@link UserSubmittedPromise} ended up.
   */
  submit(data: UserSubmittedPromise<T,R,D>): SubmissionResult<R> {
    const submittedPromise = this.copySubmittedPromise(data);
    const newSubmissionInstructions = submittedPromise.behaviorFunctions.submissionBehavior!(new SubmissionContext(this, submittedPromise));
    return this.internalSubmit({ 
      submittedPromise, 
      instruction: newSubmissionInstructions, 
      type: 'new', 
      source: 'new', 
      executionStats: {
        totalExecutions: 0, 
        consecutiveRetries: 0, 
        totalRetries: 0
      } 
    });
  }

  //Copy the config in the UserSubmittedPromise so we can coalesce with the pool config and avoid the possibility of the user altering it after submission
  private copySubmittedPromise(submittedPromise: UserSubmittedPromise<T,R,D>): SubmittedPromise<T,R,D> {
    return {
      behaviorFunctions: {
        onResultBehavior: submittedPromise.behaviorFunctions?.onResultBehavior || this.behaviorFunctions?.onResultBehavior,
        errorBehavior: submittedPromise.behaviorFunctions?.errorBehavior || this.behaviorFunctions.errorBehavior,
        queueBehavior: submittedPromise.behaviorFunctions?.queueBehavior || this.behaviorFunctions.queueBehavior,
        returnBehavior: submittedPromise.behaviorFunctions?.returnBehavior || this.behaviorFunctions.returnBehavior,
        submissionBehavior: submittedPromise.behaviorFunctions?.submissionBehavior || this.behaviorFunctions.submissionBehavior,
        successBehavior: submittedPromise.behaviorFunctions?.successBehavior || this.behaviorFunctions.successBehavior
      },
      collectMetrics: submittedPromise.collectMetrics ?? this.collectMetrics,
      eventSilenceConfig: {...this.eventSilenceConfig, ...submittedPromise.eventSilenceConfig || {}},
      handlerFunctions: {
        metricsHandler: submittedPromise.handlerFunctions?.metricsHandler || this.handlerFunctions.metricsHandler,
      },
      logger: submittedPromise.logger ? 
        new WrappedLogger<R>(submittedPromise.logger?.logLevel ?? this.logger.level, 
          submittedPromise.logger?.jobMetadataSerializer || this.logger.serializer, 
          submittedPromise.logger?.logger) : this.logger,
      promise: submittedPromise.promise,
      maxRecurrences: submittedPromise.maxRecurrences ?? this.recurrences,
      maxRetries: submittedPromise.maxRetries ?? this.retries,
      timeout: submittedPromise.timeout ?? this.timeout,
      timestampProvider: submittedPromise.timestampProvider || this.timestampProvider,
      abortController: submittedPromise.abortController,
      jobMetadata: submittedPromise.jobMetadata,
      delay: submittedPromise.delay ?? this.delay ?? 0,
      delayType: submittedPromise.delayType ?? this.delayType ?? 'cumulative'
    };
  }

  /**
   * @returns The current global {@link EventSilenceConfig}
   */
  getEventSilenceConfig(): FullEventSilenceConfig<T,R> {
    return this.eventSilenceConfig;
  }

  /**
   * Set the current `EventSilenceConfig`. If the config does not have all event types present, then `active_with_inherit` will be assigned as the default
   * @param config An {@link EventSilenceConfig} object
   * @returns The full {@link EventSilenceConfig} that was set after assigning any defaults
   */
  setEventSilenceConfig(config: PartialEventSilenceConfig<T,R>): FullEventSilenceConfig<T,R> {
    for (const event of eventTypes) {
      if (!config[event]){
        this.eventSilenceConfig[event] = 'active_with_inherit'; //Document as default if none given
      } else {
        this.eventSilenceConfig[event] = config[event];
      }
    }
    return this.eventSilenceConfig;
  }

  /**
   * Patch the `EventSilenceConfig` with a partial object. This will only assign the fields passed and any missing fields will remain untouched on the existing config.
   * @param config An {@link EventSilenceConfig} object
   * @returns The full {@link EventSilenceConfig} set on the pool after being patched
   */
  patchEventSilenceConfig(config: PartialEventSilenceConfig<T,R>): FullEventSilenceConfig<T,R> {
    return Object.assign(this.eventSilenceConfig, config);
  }

  /**
   * Fetch the current global behavior functions
   * @returns A copy of the existing global behavior functions
   */
  getBehaviorFunctions(): InternalBehaviorFunctions<T,R,D> {
    return {...this.behaviorFunctions};
  }

  /**
   * Check to see if the pool is in a state that allows new jobs to be submitted. The states `stopped`, `halted`, and `closed` do not allow new submissions.
   * @returns `true` if the pool is in a state that allows for new submissions, `false` if not
   * @throws {RangeError} if a queue index is out of range for the current queue
   */
  canSubmitNewJobs(): boolean {
    return !this.noSubmissionStates.has(this.poolState.status);
  }

  private submitToQueue(submission: InternalSubmission<T,R,D>, insertionTime?: D, countDeferredAgainstDelay?: boolean): SubmissionResult<R> {
    const queueItem = new QueueItem<T,R,D>(this.queue, submission, this.poolState, this.pool, submission.id ?? this.poolState.poolIdCounter++, this.internalSubmit.bind(this), insertionTime, countDeferredAgainstDelay);
    let destination = submission.instruction.destination;
    if (destination === 'back_of_queue' || destination === 'pool' || destination === 'deferred_queue'){ //deferred_queue due to resubmission
      this.queue.push(queueItem);
      destination = 'back_of_queue';
    } else if (destination === 'front_of_queue'){
      this.queue.unshift(queueItem);
    } else if (destination === 'queue_index'){
      const index = submission.instruction.index!;
      if (index < 0 || index > this.queue.length){
        throw new RangeError(`Queue index outside acceptable range, must be between 0 and ${this.queue.length} (current queue length), got ${index}`);
      }
      this.queue.splice(submission.instruction.index!, 0, queueItem);
    }
    queueItem.init();
    submission.submittedPromise.logger.debug?.(`Job has been submitted to the queue`, submission.submittedPromise.jobMetadata);
    return {destination: destination || submission.instruction.destination, id: queueItem.id, jobMetadata: submission.submittedPromise.jobMetadata};
  }

  private internalSubmit(submission: InternalSubmission<T,R,D>): SubmissionResult<R> {
    submission.submissionTime = submission.submissionTime ?? submission.submittedPromise.timestampProvider.now();
    const logger = submission.submittedPromise.logger;
    if (!submission.instruction.destination || submission.instruction.destination === 'none'){
      logger.debug?.(`Submission destination is none, skipping`, submission.submittedPromise.jobMetadata);
      return {destination:'none', id: submission.id, jobMetadata: submission.submittedPromise.jobMetadata}; //No point in going further than this.
    }
    if (submission.type !== 'retry'){
      submission.executionStats.consecutiveRetries = 0;
    }
    if (submission.source !== 'pool'){
      submission.instruction = this.determineDestination(submission.instruction, submission.source);
    }
    //Since IDs are reused for recurrences/retries, not having an ID means a new submission, which means it goes into the deferred queue if we're awaiting.
    if (this.isAwaitingResults() && (submission.id == undefined || submission.instruction.destination === 'deferred_queue')){
      if (!submission.id){ //Assign an ID so we have something to pass back if it's a new submission.
        submission.id = this.poolState.poolIdCounter++;
      }
      submission.submittedPromise.logger.debug?.('Submitting job sent to deferred queue due to awaiting state', submission.submittedPromise.jobMetadata);
      this.deferredQueue.push({
        submission,
        insertionTime: this.timestampProvider.now(),
        countDeferredAgainstDelay: !!submission.instruction.countDeferredAgainstDelay
      });
      return { destination: 'deferred_queue', id: submission.id, jobMetadata: submission.submittedPromise.jobMetadata };
    }
    if (this.queueDestinations.has(submission.instruction.destination) || this.poolState.status === 'paused'){
      return this.submitToQueue(submission);
    } else {
      const id = submission.id ?? this.poolState.poolIdCounter++;
      const runId = submission.runId ? ++submission.runId : 1;
      //Because we want to be certain that the promise doesn't start running until the wrapper is in the map
      const promise = new PoolPromise(submission, this.processResult.bind(this), id, runId, this.invokeListeners.bind(this), submission.submissionTime);
      this.pool.set(id, promise);
      promise.execute();
      logger.debug?.(`Job added to pool with ID ${id}`, submission.submittedPromise.jobMetadata);
      return {destination: 'pool', id, jobMetadata: submission.submittedPromise.jobMetadata};
    }
  }

  private determineDestination(instruction: AwaitDestinationInstructionWithFallback, source: InternalSubmissionSource): AwaitDestinationInstruction {
    //If we want force_pool but there's no room then use the fallback or default to front of queue.
    const fallback = instruction.fallbackInstruction ? { ...instruction, ...instruction.fallbackInstruction} : undefined;
    if (instruction.destination === 'force_pool' && this.pool.size >= this.poolState.poolSize){
      if (fallback){
        return this.determineDestination(fallback, source);
      }
      return { ...instruction, destination: 'front_of_queue'};
      //If we want pool but the pool is full or there's one slot open but it's a retry/recurrence, then use the fallback or default it to the back of the queue
    } else if (instruction.destination === 'pool' && (this.pool.size >= this.poolState.poolSize || (this.pool.size >= this.poolState.poolSize - 1 && source === 'pool'))) {
      if (fallback) {
        return this.determineDestination(fallback, source);
      }
      return {...instruction, destination: 'back_of_queue'};
      //If we want the queue but there are openings in the pool and no queue-specific delay, then use the fallback or put it in the pool.
    } else if (this.queueDestinations.has(instruction.destination) && this.pool.size < this.poolState.poolSize && (!instruction.delay || instruction.delayType !== 'queue')){
      if (fallback) {
        return this.determineDestination(fallback, source);
      }
      return {...instruction, destination: 'pool'};
    }
    return instruction;
  }

  private processResult(jobResult: WrappedResult<T,R,D>): WrappedResult<T,R,D> {
    const logger = jobResult.submittedPromise.logger;
    const metadata = jobResult.submittedPromise.jobMetadata;
    const promise = this.pool.get(jobResult.id);
    this.pool.delete(jobResult.id);
    const context = new ResultContext(this, jobResult);
    let instruction: SuccessOrFailInstruction<T, R, D> | undefined;
    let destination: AwaitDestinationInstruction | undefined;
    let hasError: boolean = false;
    if (promise && promise.status !== 'halted'){
      hasError = promise.status === 'error';
      const isAwaiting = this.isAwaitingResults();
      
      if (hasError) { //The eventSilenceConfig options will be there since we coalesce with the full global eventSilenceConfig 
        this.invokeListeners('job_error', jobResult.submittedPromise.eventSilenceConfig.job_error!, [jobResult.result?.results[0]?.error, metadata]);
        instruction = isAwaiting ?
          this.awaitingState.awaitConfig!.errorBehavior(new AwaitContext(this, jobResult, this.awaitingState.awaitConfig!.executionStats, this.awaitingState.awaitConfig!.maxRecurrences, this.awaitingState.awaitConfig!.maxRetries)) :
          jobResult.submittedPromise.behaviorFunctions.errorBehavior(context);
        logger.debug?.('Job returned with error, setting the errorBehavior handler', metadata);
      } else {
        this.invokeListeners('job_success', jobResult.submittedPromise.eventSilenceConfig.job_success!, [jobResult.result, metadata]);
        instruction = isAwaiting ?
          this.awaitingState.awaitConfig!.successBehavior(new AwaitContext(this, jobResult, this.awaitingState.awaitConfig!.executionStats, this.awaitingState.awaitConfig!.maxRecurrences, this.awaitingState.awaitConfig!.maxRetries)) :
          jobResult.submittedPromise.behaviorFunctions.successBehavior(context);
        logger.debug?.('Job returned with success, setting the successBehavior handler', metadata);
      }
      destination = this.determineDestination(instruction.destinationInstruction, 'pool');
      try {
        if (jobResult.status !== 'halted' && jobResult.submittedPromise.behaviorFunctions.returnBehavior(context)) {
          let mapResult: PoolResult<T, R> | undefined = this.results.get(jobResult.id);
          if (!mapResult) {
            mapResult = jobResult.result!;
            this.results.set(jobResult.id, mapResult);
          } else {
            mapResult.results.push(jobResult.result!.results[0]);
          }
        }
      } catch (err) { //Not much else we can do here without blowing out the pool entirely.
        logger.error?.(`Error when executing returnBehavior function`, metadata, err);
      }
      if (jobResult.submittedPromise.handlerFunctions.metricsHandler || jobResult.submittedPromise.collectMetrics){
        const fullMetrics: FullMetrics<R,D> = {...jobResult.metrics, endTime: jobResult.submittedPromise.timestampProvider.now() };
        if (jobResult.submittedPromise.handlerFunctions.metricsHandler){
          this.deferTask(() => { //Fire and forget so we don't wait for it
            try {
              jobResult.submittedPromise.handlerFunctions.metricsHandler!(fullMetrics);
            } catch (err: unknown) {
              logger.error?.('Metric handler failed', metadata, err);
            }
          });
        }
        if (jobResult.submittedPromise.collectMetrics) {
          let metricArray = this.jobMetrics.get(jobResult.id);
          if (!metricArray){
            metricArray = [];
            this.jobMetrics.set(jobResult.id, metricArray);
          }
          metricArray.push(fullMetrics);
        }
      }
      try {
        if (instruction?.status) {
          this.setStatus(instruction.status, instruction.options);
        }
      } catch (err) { //Not much to do here. This should be nigh-impossible anyway.
        logger.error?.(`Error when setting status to ${instruction?.status} in handleResubmission`, metadata, err);
      }
      if (jobResult.status === 'complete') { //Note that halted jobs are not counted in pool metrics
        this.poolMetrics.succeeded++;
      } else if (jobResult.status === 'error') {
        this.poolMetrics.failed++;
      }
      logger.debug?.('Job is being resubmitted', metadata, instruction);

      this.internalSubmit({
        submittedPromise: jobResult.submittedPromise,
        id: jobResult.id,
        runId: jobResult.runId,
        instruction: destination!,
        executionStats: jobResult.executionStats,
        type: instruction!.destinationInstruction.destination !== 'none' ? (hasError ? 'retry' : 'recurrence') : 'none',
        source: 'pool'
      });
    } else {
      logger.debug?.('Job halted prior to processing result, skipping', metadata);
    }

    
    if (destination?.destination !== 'force_pool' && destination?.destination !== 'pool' && this.queue.length){
      this.submitNext(context, jobResult.submittedPromise.behaviorFunctions.queueBehavior);
    } else if (!this.pool.size) {
      logger.debug?.('Pool is empty after running job', metadata);
      this.invokeListeners('pool_empty', jobResult.submittedPromise.eventSilenceConfig.pool_empty!, []);
    }
    
    return jobResult;
  }

  private submitNext(context: IQueueContext<T,R,D>, queueBehaviorFunction: QueueBehaviorFunc<T,R,D>): boolean {
    const queueInstruction = queueBehaviorFunction(context);
    if ((queueInstruction.value || (queueInstruction.index != undefined && queueInstruction.index >= 0)) && this.poolState.poolSize > this.pool.size){
      let nextItem = queueInstruction.value as InternalQueueItem<T,R,D>;
      if (!nextItem){
        nextItem = this.queue[queueInstruction.index!];
      }
      const index = this.queue.findIndex((q) => q.id === nextItem.id);
      if (index >= 0){
        this.queue.splice(index, 1);
      } else {
        nextItem.submission.submittedPromise.logger.warn?.('Invalid index given for queue item', nextItem.submission.submittedPromise.jobMetadata, {index, queueId: nextItem.id});
      }
      let remaining = 0;
      if (nextItem.getStatus() === 'delayed'){
        remaining = nextItem.clearTimer();
        nextItem.submission.submittedPromise.logger.debug?.(`Clearing timer for queue item, ${remaining} ms remaining on delay`, nextItem.submission.submittedPromise.jobMetadata, {queueId: nextItem.id});
      }
      const instruction = nextItem.submission.instruction;
      let delay = instruction.delay ?? nextItem.submission.submittedPromise.delay;
      const delayType = instruction.delayType ?? nextItem.submission.submittedPromise.delayType;
      if (delayType === 'cumulative' && remaining > 0){
        delay = remaining;
      }
      this.internalSubmit({
        type: nextItem.submission.type,
        source: 'queue',
        submittedPromise: nextItem.submission.submittedPromise, 
        executionStats: nextItem.submission.executionStats,
        submissionTime: nextItem.submission.submissionTime,
        ...(nextItem.submission.resubmitDuringAwaiting ? {resubmitDuringAwaiting: nextItem.submission.resubmitDuringAwaiting} : {}),
        ...(nextItem.submission.queueInsertionTime ? {queueInsertionTime: nextItem.submission.queueInsertionTime} : {}),
        ...(nextItem.submission.fallbackInstruction ? {...nextItem.submission.fallbackInstruction} : {}),
        instruction: {
          destination: 'pool',
          delay,
          delayType
        },
        id: nextItem.id,
        runId: nextItem.submission.runId
      });
      return true;
    } else {
      context.promisePool.logger.debug?.('No value returned from submitNext');
    }
    return false;
  }
  
  /**
   * This puts the pool into a specific state to await results. This will take the current state of the pool and hold it until the pool and queue are drained.
   * Any new jobs will be placed into a deferred queue and will not be processed until this function completes. Subsequent calls to this function while the
   * pool is still in an awaiting state will ignore the config and immediately return the same promise. It will also clear the `results` array if used and 
   * return the results in the `AwaitResults`, so plan accordingly.
   * @param awaitConfig An {@link AwaitConfig} object to set await behavior
   * @returns A promise for an {@link AwaitResults} object containing an array of results from the pool as well as additional information. Be aware that this
   * will return ALL results stored in the pool, not just the results that were collected after `awaitResults` was called.
   * @throws `PoolError` if the `onQueueStall` function returns a `throw` error.
   */
  async awaitResults(awaitConfig?: AwaitConfig<T,R,D>): Promise<AwaitResults<T,R>> {
    if (this.awaitingState.awaitingPromise){
      this.logger.debug?.('awaitResults called while pool is already in awaiting state, returning original promise');
      return this.awaitingState.awaitingPromise;
    }
    if (this.poolState.status === 'halted' || this.poolState.status === 'paused' || this.poolState.status === 'stopped'){
      this.logger.debug?.(`awaitResults called while pool status is ${this.poolState.status}, which is invalid. Returning empty and invalid result`);
      return Promise.resolve({results: [], status: 'invalid', reason: `Current pool state is ${this.poolState.status}`});
    }
    if (!this.pool.size && !this.queue.length) {
      this.logger.debug?.('awaitResults called while pool is empty, returning empty results');
      return Promise.resolve(this.postAwaitCleanup("empty"));
    }
    //Grab a snapshot of the current execution stats for all items in the pool/queue
    const executionsMap = new Map<number, ExecutionStats>();
    for (const [id, job] of this.pool.entries()){
      executionsMap.set(id, job.executionStats); //Already copied/frozen
    }
    for (const item of this.queue){
      executionsMap.set(item.id, Object.freeze({...item.submission.executionStats}));
    }
    //Copy to avoid the user altering this outside of the pool and assign defaults
    const newConfig: InternalAwaitConfig<T,R,D> = {
      errorBehavior: awaitConfig?.errorBehavior || DefaultAwaitErrorBehavior,
      successBehavior: awaitConfig?.successBehavior || DefaultAwaitSuccessBehavior,
      onQueueStall: awaitConfig?.onQueueStall || (() => ({action: 'abort'})),
      maxRecurrences: awaitConfig?.maxRecurrences ?? 0,
      maxRetries: awaitConfig?.maxRetries ?? 0,
      executionStats: new AwaitExecutionStats(executionsMap),
      deferredQueueBehavior: awaitConfig?.deferredQueueBehavior || DefaultDeferredQueueBehavior
    };

    this.awaitingState.awaitAbortController = new AbortController();
    this.awaitingState.awaitConfig = newConfig;

    const abortPromise = new Promise<void>((abortResolve) => {
      const onAbort = () => {
        abortResolve(); //Don't throw an error, just end it
        if (this.awaitingState.awaitAbortController?.signal.reason !== 'finally'){
          this.logger.debug?.('awaitResults was aborting before completing');
        }
      }
      this.awaitingState.awaitAbortController?.signal.addEventListener('abort', onAbort, {once: true});
    });
    this.awaitingState.awaitingResults = 'awaiting';

    const awaitingPromise = new Promise<AwaitResults<T,R>>(async (resolve) => {
      try {
        while (this.queue.length || this.pool.size) {
          if (this.queue.length && !this.pool.size){ //Edge case: Pool empty, nothing but delayed items in queue
            this.logger.debug?.('Pool is empty but events are in the queue');
            if (!this.submitNext(new QueueContext(this), this.behaviorFunctions.queueBehavior)){
              if (this.queue.some((q) => q.getStatus() === 'delayed')){
                let jobStartEvent: () => void;
                let listenerId: number;
                this.logger.debug?.('No item returned from awaitQueueBehavior function, waiting for delays to pick up');
                await Promise.race([new Promise<void>((waitResolve) => {
                  jobStartEvent = () => {
                    waitResolve();
                    this.logger.debug?.('Awaited job has entered the pool');
                  };
                  listenerId = this.internalAddListener('job_start', jobStartEvent, true);
                }), abortPromise]).finally(() => {
                  if (listenerId !== undefined){
                    this.internalRemoveListener('job_start', jobStartEvent, true);
                  }
                });
              } else { //Edge case: User has an odd queueBehaviorFunction that skips over non-delayed queue items.
                const action = this.awaitingState.awaitConfig?.onQueueStall();
                if (action?.action === 'wait'){
                  await Promise.race([
                    new Promise<void>((waitResolve) => {
                      setTimeout(() => {
                        waitResolve();
                      }, action.time);
                    }),
                    abortPromise
                  ]);
                } else if (action?.action === 'throw'){
                  throw new PoolError<R>('Await stalled with an empty pool, undelayed items in the queue, and no results from the queue behavior function');
                } else if (action?.action === 'abort'){
                  this.logger.warn?.('Aborting awaitResults due to empty pool and non-delayed queue items unreturned by queue behavior function');
                  const results = this.postAwaitCleanup('aborted', 'Aborted by onQueueStall function');
                  resolve({results: results.results, status: results.status, reason: results.reason});
                  return;
                }
              }
            }
          }
          //Race in case the pool is shut down while we're waiting.
          await Promise.race(
            [Promise.all([...this.pool.values()].map((p) => p.runningPromise)), abortPromise]);
        }
        this.logger.debug?.('Await cycle complete, cleaning up');
        const results = this.postAwaitCleanup();
        resolve({results: results.results, status: results.status, reason: results.reason});
      } catch (err) {
        const results = this.postAwaitCleanup();
        resolve({results: results.results, status: 'error', reason: results.reason, error: err});
      }
    }).finally(() => {
      if (!this.awaitingState.awaitAbortController?.signal.aborted){ //Don't leave this promise dangling
        this.awaitingState.awaitAbortController?.abort('finally');
      }
      delete this.awaitingState.awaitAbortController;
      delete this.awaitingState.awaitingPromise;
      delete this.awaitingState.awaitConfig;
    });
    this.awaitingState.awaitingPromise = awaitingPromise;
    return this.awaitingState.awaitingPromise;
  }

  private postAwaitCleanup(status?: AwaitingStatus, reason?: string): {results: PoolResult<T,R>[], status: AwaitingStatus, reason: string} {
    const resultsToReturn = [...this.results.values()];
    this.results.clear();
    this.awaitingState.awaitingResults = 'none';
    if (this.poolState.status !== 'halted' && this.poolState.status !== 'stopped'){
      for (const item of this.awaitingState.awaitConfig!.deferredQueueBehavior(new DeferredQueueContext(this, this.deferredQueue))){
        this.internalSubmit({
          ...item.item.submission,
          instruction: item.instruction,
          source: 'new'
        });
      }
    }
    this.deferredQueue.length = 0;
    if (this.awaitingState.awaitAbortController?.signal?.aborted){
      status = 'aborted';
    } else if (this.poolState.status === 'halted'){
      status = 'halted';
    }
    return {results:resultsToReturn, status: status || 'complete', reason: this.awaitingState.awaitAbortController?.signal?.reason || reason};
  }

  /**
   * Cancel the current awaiting state. This is effectively a no-op if the pool is not in an awaiting state.
   * @param reason Anything that you want to be passed as a reason to the abort controller
   */
  cancelAwaitResults(reason?: unknown): void {
    this.awaitingState?.awaitAbortController?.abort(reason || 'awaitResults was manually canceled');
  }

  private resumePool(queueBehaviorFunction?: QueueBehaviorFunc<T,R,D>) {
    if (this.pool.size < this.poolState.poolSize){
      const context = new QueueContext(this);
      while (this.submitNext(context, queueBehaviorFunction || this.behaviorFunctions.queueBehavior) && this.pool.size < this.poolState.poolSize){} //Empty while loop to simply iterate until it's done
    }
  }

  private clearQueues(closeType: PoolCloseType): void {
    if (closeType !== 'continue'){
      this.logger.debug?.(`Clearing queues due to closing: ${closeType}`);
      this.deferredQueue.length = 0;
      for (const item of this.queue){
        item.clearTimer();
      }
      this.queue.length = 0;
    }
  }

  private clearPool(closeType: PoolCloseType): void {
    if (closeType !== 'continue'){
      this.clearQueues(closeType);
      if (closeType !== 'clear_queue'){
        this.logger.debug?.(`Clearing pool due to closing: ${closeType}`);
        for (const promise of this.pool.values()) {
          promise.cancel(closeType === 'immediate');
        }
      }
    }
  }

  /**
   * Manually sets the status of the pool with some options to dictate behavior. Be aware that `stopped` and `halted` are terminal states and attempting to
   * change from a terminal state to any other state will be ignored.
   * @param status The new {@link PoolStatus}
   * @param options {@link PoolStatusOptions} to determine behavior on certain state changes
   */
  setStatus(status: PoolStatus, options?: PoolStatusOptions<T,R,D>): void {
    const oldStatus = this.poolState.status;
    if (options?.eventSilenceConfig) {
      this.patchEventSilenceConfig(options.eventSilenceConfig);
    }
    if (oldStatus === status){
      this.logger.debug?.(`setStatus called with identical status ${status}`);
      return;
    }
    this.invokeListeners('pool_state_change', this.eventSilenceConfig.pool_state_change, [oldStatus, status, options?.message]);
    this.logger.debug?.(`State changing from ${oldStatus} to ${status}`);
    if (oldStatus === 'running') { //Any other status is valid from here
      this.poolState.status = status;
      if (status === 'stopped' || status === 'halted') {
        const closeType = status === 'halted' ? 'immediate' : options?.closeType || 'immediate';
        this.clearPool(closeType);
      }
    } else if (oldStatus === 'closed') {
      this.poolState.status = status;
      if (status === 'paused' || status === 'running') {
        this.resumePool(options?.queueBehaviorFunction);
      } else if (status === 'stopped' || status === 'halted') {
        const closeType = status === 'halted' ? 'immediate' : options?.closeType || 'immediate';
        this.clearPool(closeType);
      }
    } else if (oldStatus === 'paused') {
      this.poolState.status = status;
      if (status === 'running' || status === 'closed') {
        this.resumePool(options?.queueBehaviorFunction);
      } else if (status === 'stopped' || status === 'halted'){
        const closeType = status === 'halted' ? 'immediate' : options?.closeType || 'immediate';
        this.clearPool(closeType);
      }
    } else if (oldStatus === 'halted' || oldStatus === 'stopped') {
      this.logger.warn?.(`Invalid pool state change, ignoring`);
    }
    if (status === 'halted'){
      this.handlerFunctions.onPoolHalt?.(new PoolError(`Pool halted with error: ${options?.message}`));
    }
  }

  getCountOfRunningJobs(): number {
    return this.pool.size;
  }

  getQueueSize(): number {
    return this.queue.length;
  }

  /**
   * @returns A copy of the existing {@link FullMetrics} map. Don't alter the arrays.
   */
  getRawMetrics(): Map<number, FullMetrics<R,D>[]> {
    return new Map<number, FullMetrics<R,D>[]>(this.jobMetrics.entries());
  }

  /**
   * Pass an `IteratorFunction` here to iterate over the running jobs in the pool and perform some action on them.
   * @param fn An {@link IteratorFunction} to operate on each {@link IPoolPromise} in the pool
   */
  iterateRunningJobs(fn: IteratorFunction<IPoolPromise<R>>): void {
    const poolValues = [...this.pool.values()];
    for (let i = 0; i < poolValues.length; i++){
      if (fn(poolValues[i], i)){
        break;
      }
    }
  }

  /**
   * Pass an `IteratorFunction` here to iterate over the waiting jobs in the queue and perform some action on them.
   * @param fn An {@link IteratorFunction} to operate on each {@link IQueueItem} in the queue
   */

  iterateQueue(fn: IteratorFunction<IQueueItem<R>>): void {
    for (let i = 0; i < this.queue.length; i++) {
      if (fn(this.queue[i], i)) {
        break;
      }
    }
  }

  /**
   * Delete the collected {@link FullMetrics} if present
   */
  clearMetrics(): void {
    this.jobMetrics.clear();
  }

  /**
   * Return a set of calculated {@link PoolMetrics}. This does not clear the existing collected metrics. Use {@link clearMetrics()} to clear them.
   * @returns A {@link PoolMetrics} object containing various metrics for the pool
   */
  getPoolMetrics(): PoolMetrics {
    return { ...this.poolMetrics,
      runningJobs: this.pool.size,
      queueSize: this.queue.length,
    };
  }

  /**
   * Clears the metrics for total succeeded/failed jobs
   */
  clearPoolMetrics(): void {
    this.poolMetrics.failed = 0;
    this.poolMetrics.succeeded = 0;
  }

  getPoolSize(): number {
    return this.poolState.poolSize;
  }

  /**
   * Set a new size for the pool. If the new size is larger then the queue will be immediately consumed until the pool is full again. If the new size is
   * smaller then existing jobs will complete, but new jobs will not be pulled from the queue until the pool falls below the new size.
   * @param poolSize The new size of the pool
   * @param queueBehaviorFunction An option {@link QueueBehaviorFunc} that will determine how the queue is consumed if the new size is larger than the old.
   * This function applies ONLY to this call and will not be retained.
   * @throws `PoolError` if the size is zero or less
   */
  setPoolSize(poolSize: number, queueBehaviorFunction?: QueueBehaviorFunc<T,R,D>): void {
    if (poolSize <= 0){
      throw new PoolError("Pool size must be greater than zero");
    }
    const oldSize = this.poolState.poolSize;
    this.poolState.poolSize = poolSize;
    if (oldSize < poolSize && this.canSubmitNewJobs()) { //If we're expanding the pool then pull from the queue
      this.logger.debug?.(`Expanding the pool from ${oldSize} to ${poolSize}, adding new jobs`);
      let iterations = poolSize - oldSize;
      const context = new QueueContext(this);
      for (iterations; iterations > 0; iterations--) {
        this.submitNext(context, queueBehaviorFunction || this.behaviorFunctions.queueBehavior);
      }
    }
  }

  getStatus(): PoolStatus {
    return this.poolState.status;
  }

  /** 
   * @returns `true` if {@link awaitResults()} has been called and is still processing, `false` if not. 
   */
  isAwaitingResults(): boolean {
    return this.awaitingState.awaitingResults === 'awaiting';
  }

  /**
   * Clear any retained job results. This will not clear results if the pool is in an awaiting state.
   */
  clearResults(): void {
    if (!this.isAwaitingResults()){
      this.results.clear();
    }
  }

  /**
   * 
   * @returns A copy of the current retained job results.
   */
  getCurrentResults(): PoolResult<T,R>[] {
    return [...this.results.values()];
  }

  /**
   * Return the current job results and clear the array.
   * @returns A copy of the current retained job results.
   */
  getCurrentResultsAndClear(): PoolResult<T,R>[] {
    const results = [...this.results.values()];
    this.results.clear();
    return results;
  }

  /**
   * Add an event listener to the pool
   * @param event The {@link PoolEvent}
   * @param listener An event-specific function to execute when an event occurs
   * @param instant If this event should be fired instantly (`true`) or via `setImmediate()` (`false`). Defaults to `false`. It is recommended
   * to not set this to `true` unless you need an event fired off at the exact time it occurs as longer-running listeners can skew metrics.
   * @returns A numeric id for the submitted event
   */
  addListener<K extends keyof PoolEvent<T,R>>(event: K, listener: PoolEvent<T,R>[K], instant?: boolean): number {
    return this.internalAddListener(event, listener, false, instant);
  }

  private internalAddListener<K extends keyof PoolEvent<T,R>>(event: K, listener: PoolEvent<T,R>[K], internal: boolean, instant?: boolean): number {
    if (!this.listeners[event]) {
      this.listeners[event] = [];
    }
    this.poolState.eventIdCounter++;
    this.listeners[event].push({id: this.poolState.eventIdCounter, func: listener, internal, type: event, instant});
    return this.poolState.eventIdCounter;
  }

  /**
   * Remove a specific event listener
   * @param event The {@link PoolEvent}
   * @param listener Either the numeric id returned from {@link addListener()} or the original event listener function that was passed.
   * @returns 'true' if the listener was successfully removed, `false` if it was not succesfully removed.
   */
  removeListener<K extends keyof PoolEvent<T,R>>(event: K, listener: number | PoolEvent<T,R>[K]): boolean {
    return this.internalRemoveListener(event, listener, false);
  }

  private internalRemoveListener<K extends keyof PoolEvent<T,R>>(event: K, listener: number | PoolEvent<T,R>[K], removeInternal: boolean): boolean {
    if (!this.listeners[event]) {
      return false;
    }
    let index = -1;
    const events = this.listeners[event];
    if (typeof listener === 'number'){
      index = events.findIndex((e) => e.id === listener);
    } else {
      index = events.findIndex((e) => e.func === listener);
    }
    if (index > -1){
      if (!removeInternal && events[index].internal) {
        return false;
      }
      events.splice(index, 1);
      return true;
    }
    return false;
  }

  private invokeListeners<K extends EventKey>(event: K, mode: SilenceMode, payload: Parameters<PoolEvent<T,R>[K]>): void {
    let listeners: PoolListener<T,R,K>[] = [];
    if (mode === 'force_silence' || 
        this.eventSilenceConfig[event] === 'force_silence' || 
        (this.eventSilenceConfig[event] === 'silent_with_inherit' && (!mode || mode === 'silent_with_inherit'))){
      listeners = this.listeners[event]?.filter((e) => e.internal) || [];
    } else {
      listeners = this.listeners[event] || [];
    }
    listeners.forEach(listener => {
      if (!listener.instant){
        this.deferTask(() => { //Set immediate so we don't even have to worry about waiting for any synchronous execution to complete, just fire and forget.
          this.executeListener(listener, payload);
        });
      } else {
        this.executeListener(listener, payload);
      }
    });
  }

  private executeListener<K extends EventKey>(listener: PoolListener<T, R, K>, payload: Parameters<PoolEvent<T, R>[K]>): void {
    try {
      (listener.func as (...args: unknown[]) => void)(...payload);
    } catch (err) {
      this.logger.error?.(`Error in ${event} event listener with ID ${listener.id}`, undefined, err);
    }
  }
  //Platform-agnostic wrapper so this should be able to run in a web browser as well as node.
  private deferTask = (callback: () => void): void => {
    if (typeof setImmediate === 'function') {
      setImmediate(callback);
    } else {
      setTimeout(callback, 0);
    }
  };
}

type InternalAwaitConfig<T, R, D> = {
  executionStats: AwaitExecutionStats<T, R, D>;
} & Required<AwaitConfig<T, R, D>>;

/**
 * Default {@link TimestampProvider} for the pool. Uses Date.now() and standard arithmatic so we're always working in ms.
 */
const DefaultTimestampProvider: TimestampProvider = {
  now(): number { //This must be able to be converted to the standard epoch time
    return Date.now();
  },

  subtract(num1: number, num2: number): number {
    return num1 - num2;
  },

  toMs(num: number): number { //This conversion could deal with a small number, ie, 5ms, or it could be an epoch timestamp. Handle accordingly.
    return num;
  }
} as const;

type AwaitingState<T, R, D> = {
  awaitingResults: 'none' | 'awaiting';
  awaitAbortController?: AbortController;
  awaitingPromise?: Promise<AwaitResults<T, R>>;
  awaitConfig?: InternalAwaitConfig<T, R, D>;
};

type InternalBehaviorFunctions<T, R, D> = Required<BehaviorFunctions<T, R, D>>;

type PoolListener<T, R, K extends EventKey> = { 
  id: number, 
  func: PoolEvent<T, R>[K], 
  internal?: boolean, 
  type: K,
  instant?: boolean
};
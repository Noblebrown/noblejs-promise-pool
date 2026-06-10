import { IAwaitContext, IBaseContext, IDeferredQueueContext, IQueueContext, IResultContext, ISubmissionContext } from "../types/behavior";
import { SubmittedPromise, WrappedResult } from "../types/config";
import { AwaitExecutionStats } from "./await-execution-stats";
import { DeferredQueueItem } from "./internal";
import type { PromisePool } from "./promise-pool";

class BaseContext<T, R, D> implements IBaseContext<T, R, D> {
  constructor(
    public readonly promisePool: PromisePool<T, R, D>,
  ) {
  }
}

export class SubmissionContext<T, R, D> extends BaseContext<T, R, D> implements ISubmissionContext<T, R, D> {
  constructor(
    public readonly promisePool: PromisePool<T, R, D>,
    public readonly submittedPromise: SubmittedPromise<T, R, D>
  ) {
    super(promisePool);
  }
}

export class ResultContext<T, R, D> extends BaseContext<T, R, D> implements IResultContext<T, R, D> {
  constructor(
    public readonly promisePool: PromisePool<T, R, D>,
    public readonly result: WrappedResult<T, R, D>,
  ) {
    super(promisePool);
  }
}

export class AwaitContext<T, R, D> extends ResultContext<T, R, D> implements IAwaitContext<T,R,D> {
  constructor(
    public readonly promisePool: PromisePool<T, R, D>,
    public readonly result: WrappedResult<T, R, D>,
    public readonly frozenAwaitStats: AwaitExecutionStats<T, R, D>,
    public readonly maxRecurrences: number,
    public readonly maxRetries: number
  ) {
    super(promisePool, result);
  }
}

export class QueueContext<T, R, D> extends BaseContext<T, R, D> implements IQueueContext<T, R, D> {
  constructor(
    public readonly promisePool: PromisePool<T, R, D>,
    public readonly result?: WrappedResult<T, R, D>,
  ) {
    super(promisePool);
  }
}

export class DeferredQueueContext<T,R,D> extends BaseContext<T,R,D> implements IDeferredQueueContext<T,R,D>{
  public readonly deferredQueue: DeferredQueueItem<T,R,D>[];
  constructor(
    public readonly promisePool: PromisePool<T,R,D>,
    deferredQueue: DeferredQueueItem<T, R, D>[]
  ){
    super(promisePool);
    this.deferredQueue = [...deferredQueue];
  }
}
import { DelayType } from "../types/behavior";
import { JobMetadata, QueueItemStatus, SubmissionResult } from "../types/config";
import { InternalQueueItem, InternalSubmission, PoolState } from "./internal";
import { PoolPromise } from "./pool-promise";

/**
 * This class wraps and manages jobs that have been placed in the queue
 */
export class QueueItem<T, R, D> implements InternalQueueItem<T, R, D> {
  private wakeupTimer!: ReturnType<typeof setTimeout>;
  private status: QueueItemStatus;
  public priority?: number;
  private fullDelay: number;
  private delayType: DelayType;

  constructor(
    private readonly queue: InternalQueueItem<T, R, D>[],
    public readonly submission: InternalSubmission<T, R, D>,
    private readonly poolState: PoolState,
    private readonly pool: Map<number, PoolPromise<T, R, D>>,
    public readonly id: number,
    private readonly internalSubmit: (submission: InternalSubmission<T, R, D>) => SubmissionResult<R>,
    queueInsertionTime?: D,
    countDeferredAgainstDelay?: boolean
  ) {
    if (countDeferredAgainstDelay){ //We shouldn't see an empty queueInsertionTime in this case, but since this can be overridden by the user it's possible for something to go wrong.
      this.submission.queueInsertionTime = queueInsertionTime || submission.submittedPromise.timestampProvider.now();
    } else {
      this.submission.queueInsertionTime = submission.submittedPromise.timestampProvider.now()
    }
    this.status = submission.instruction.delay && submission.instruction.delayType !== 'pool' ? 'delayed' : 'waiting';
    this.priority = submission.instruction.priority;
    this.fullDelay = submission.instruction.delay ?? submission.submittedPromise.delay;
    this.delayType = submission.instruction.delayType ?? submission.submittedPromise.delayType;
  }

  init(): void {
    const ts = this.submission.submittedPromise.timestampProvider;
    if (this.status === 'delayed' && this.submission.instruction.delayType !== 'pool') {
      const delay = ts.toMs(ts.subtract(ts.now(), this.submission.queueInsertionTime!));
      if (delay < this.submission.instruction.delay!) {
        this.createTimer(this.submission.instruction.delay! - delay);
      } else {
        this.status = 'waiting';
      }
    }
  }

  getJobMetadata(): JobMetadata<R> | undefined {
    return this.submission.submittedPromise.jobMetadata;
  }

  private createTimer(delay: number): void {
    this.wakeupTimer = setTimeout(() => { //Hedge against the queue + pool being otherwise empty before the retry timer hits. In that case, pull it out and submit it here.
      if (this.status === 'canceled') {
        const index = this.queue.findIndex((i) => i.id === this.id);
        if (index >= 0) {
          this.queue.splice(index, 1);
        }
        return;
        //Timing edge case: timeout has been cancelled, but timer has already fired and is in the event loop.
      } else if (this.status === 'waiting') {
        return;
      }
      if (this.getRemainingDelay() > 0) {
        return;
      }
      this.status = 'waiting';
      if (this.poolState.status === 'paused') {
        return;
      } else if ((this.poolState.status === 'running' || this.poolState.status === 'closed') && this.pool.size < this.poolState.poolSize) {
        const index = this.queue.findIndex((i) => i.id === this.id);
        if (index >= 0) {
          this.queue.splice(index, 1);
        } else {
          return;
        }
        if (this.poolState.poolSize > this.pool.size){
          this.internalSubmit({
            ...this.submission,
            source: 'queue',
            instruction: { ...this.submission.instruction, destination: 'pool', delay: this.fullDelay, delayType: this.delayType }
          });
        }
      }
    }, delay);
  }

  clearTimer(): number {
    return this.adjustDelay(0);
  }

  getRemainingDelay(): number {
    if (this.status === 'delayed') {
      return this.submission.submittedPromise.timestampProvider.toMs(this.submission.queueInsertionTime!) + this.fullDelay - Date.now();
    }
    return -1;
  }

  adjustDelay(newDelay: number): number { //New total delay, so 0 = stop delay
    const remaining = this.getRemainingDelay();
    if (newDelay >= 0) {
      clearTimeout(this.wakeupTimer);
      if (newDelay > 0) {
        this.fullDelay = newDelay += remaining; //Need to take existing delay into account to get the total delay time
        this.createTimer(newDelay);
      } else {
        this.status = 'waiting';
      }
      return remaining;
    }
    return -1;
  }

  getStatus(): QueueItemStatus {
    return this.status;
  }

  cancel(): number {
    const remaining = this.getRemainingDelay();
    this.status = 'canceled';
    this.adjustDelay(0);
    return remaining;
  }

  getDelayType(): DelayType | undefined {
    return this.submission.instruction.delayType;
  }
}
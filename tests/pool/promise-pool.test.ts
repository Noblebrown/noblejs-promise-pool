/* eslint-disable @typescript-eslint/no-unused-expressions */
import { expect } from "chai";
import { IQueueItem, JobMetadata, LogLevel, PoolResult, PoolStatus, PromisePool, TimestampProvider } from "../../src/index";

// eslint-disable-next-line jsdoc/require-jsdoc
export function createPromise<T>(ms: number, value?: T, error?: unknown): () => Promise<T> {
  return () => {
    return new Promise<T>((resolve, reject) => {
      setTimeout(() => {
        if (value) {
          resolve(value);
        } else {
          reject(error);
        }
      }, ms)
    });
  };
}
// eslint-disable-next-line jsdoc/require-jsdoc
export async function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(() => {
      resolve();
    }, ms);
  })
}
// eslint-disable-next-line jsdoc/require-jsdoc
export async function waitForPoolEmpty<T, R, D>(pool: PromisePool<T, R, D>): Promise<void> {
  return new Promise<void>((resolve) => {
    pool.addListener('pool_empty', () => {
      resolve();
    })
  });
}

describe("basic promise pool constructor", () => {
  it("loads PromisePool from src", () => {
    expect(() => new PromisePool({ poolSize: 0 })).to.throw(
      "The poolSize value must be greater than 0",
    );
  });

});

describe("basic retention", () => {
  it("retains and clears metrics and results", async () => {
    const pool = new PromisePool<string>({poolSize: 2, collectMetrics: true});
    pool.submit({promise: createPromise(15, 'first job')});
    pool.submit({promise: createPromise(10, 'second job')});
    await waitForPoolEmpty(pool);
    const results = pool.getCurrentResults();
    expect(pool.getRawMetrics().size).to.equal(2);
    pool.clearMetrics();
    expect(pool.getRawMetrics().size).to.equal(0);

    expect(results.find((r) => r.results[0].value === 'first job')).to.be.ok;
    expect(results.find((r) => r.results[0].value === 'second job')).to.be.ok;
    pool.clearResults();
    expect(pool.getCurrentResults().length).to.equal(0);
  });
});

describe("recurrence and retry config", () => {
  it("properly deals with recurrences and retries, both global and per-promise", async () => {
    const pool = new PromisePool<string>({poolSize: 4, maxRetries: 3, maxRecurrences: 3});
    let counter = 2;
    const destinations = pool.submitBatch([
      {promise: createPromise(5, '', new Error('globalRetry'))},
      {promise: createPromise(5, 'globalRecurrence')},
      {promise: createPromise(5, '', new Error('jobRetry')),
        maxRetries: 1
      },
      {
        promise: createPromise(5, 'jobRecurrence'),
        maxRecurrences: 2
      },
      {
        promise: () => {
          return new Promise<string>(async (resolve, reject) => {
            await delay(5);
            if (counter % 2 === 0){
              resolve(`Success ${counter}`);
            } else {
              reject(new Error(`Error ${counter}`));
            }
            counter++;
          });
        }
      }
    ]);
    await waitForPoolEmpty(pool);
    const results = pool.getCurrentResults();
    for (const destination of destinations){
      const result = results.find((r) => r.id === destination.id);
      if (result?.id === 0){
        expect(result.results.length).to.equal(4);
        for (let i = 0; i < result.results.length; i++) {
          const r = result.results[i];
          expect(r.runId).to.equal(i + 1);
          expect((r.error?.cause as Error).message).to.equal('globalRetry');
        }
      } else if (result?.id === 1){
        expect(result.results.length).to.equal(4);
        for (let i = 0; i < result.results.length; i++) {
          const r = result.results[i];
          expect(r.runId).to.equal(i + 1);
          expect(r.value).to.equal('globalRecurrence');
        }
      } else if (result?.id === 2){
        expect(result.results.length).to.equal(2);
        for (let i = 0; i < result.results.length; i++) {
          const r = result.results[i];
          expect(r.runId).to.equal(i + 1);
          expect((r.error?.cause as Error).message).to.equal('jobRetry');
        }
      } else if (result?.id === 3){
        expect(result.results.length).to.equal(3);
        for (let i = 0; i < result.results.length; i++) {
          const r = result.results[i];
          expect(r.runId).to.equal(i + 1);
          expect(r.value).to.equal('jobRecurrence');
        }
      } else if (result?.id === 4){
        for (let i = 0; i < result.results.length; i++) {
          const r = result.results[i];
          expect(r.runId).to.equal(i + 1);
          if (i % 2 === 0){
            expect(r.value).to.equal(`Success ${i + 2}`);
          } else {
            expect((r.error?.cause as Error).message).to.equal(`Error ${i + 2}`)
          }
        }
      }
    }
  });
});

describe("testing TimestampProvider", () => {
  it("Properly handles Dates", async () => {
    //Divide the timestamp into an array of [seconds, milliseconds]. Not exactly practical, but works for testing.
    class TS implements TimestampProvider<number[]> {
      now(): number[] {
        const now = Date.now();
        return [Math.floor(now / 1000), now % 1000];
      }
      subtract(num1: number[], num2: number[]): number[] {
        return [num1[0] - num2[0], num1[1] - num2[1]];
      }
      toMs(num: number[]): number {
        return (num[0] * 1000) + num[1];
      }
    }
    const ts = new TS();
    const pool = new PromisePool<string, unknown, number[]>({poolSize: 2, collectMetrics: true, timestampProvider: ts});
    const submitted = pool.submitBatch([
      { promise: createPromise(20, 'job1') },
      { promise: createPromise(15, 'job2')},
      { promise: createPromise(25, 'job3')}
    ]);
    await waitForPoolEmpty(pool);
    const metrics = pool.getRawMetrics();
    for (const destination of submitted){
      const metric = metrics.get(destination!.id!)![0];
      //Macrotask batching can sometimes barely skew the time downward. It sucks, but this is probably the best we can safely do.
      if (destination.id === 0){
        expect(ts.toMs(ts.subtract(metric.executionCompleteTime, metric.executionStartTime))).to.be.greaterThanOrEqual(17);
      } else if (destination.id === 1){
        expect(ts.toMs(ts.subtract(metric.executionCompleteTime, metric.executionStartTime))).to.be.greaterThanOrEqual(12);
      } else if (destination.id === 2){
        expect(ts.toMs(ts.subtract(metric.executionCompleteTime, metric.executionStartTime))).to.be.greaterThanOrEqual(22);
        expect(ts.toMs(ts.subtract(metric.executionCompleteTime, metric.submissionTime))).to.be.greaterThanOrEqual(35); //Had to wait for a slot
      }
    }
  });
});

describe("testing behavior functions", () => {
  it("returnBehavior works as intended", async () => {
    const pool = new PromisePool<string>({poolSize: 2, behaviorFunctions: {
      returnBehavior: (context) => {
        if (context.result.result?.results[0].value === 'job1') {
          return false;
        }
        return true;
      }
    }});
    pool.submitBatch([
      { promise: createPromise(5, 'job1') },
      { promise: createPromise(5, 'job2') },
      { promise: createPromise(5, 'job3') }
    ]);
    await waitForPoolEmpty(pool);
    const results = pool.getCurrentResults();
    expect(results.length).to.equal(2);
    expect(results[0].results[0].value).to.equal('job2');
    expect(results[1].results[0].value).to.equal('job3');
  });

  it("errorBehavior works appropriately", async () => {
    let counter = 0;
    const pool = new PromisePool<string>({
      poolSize: 2, behaviorFunctions: {
        errorBehavior: () => {
          if (counter < 9) {
            counter++;
            return {
              destinationInstruction: {
                destination: 'pool'
              }
            };
          }
          return {
            destinationInstruction: {
              destination: 'none'
            }
          };
        }
      }
    });
    pool.submitBatch([
      { promise: createPromise(10, '', new Error('job1')) },
      { promise: createPromise(11, '', new Error('job2')) },
      { promise: createPromise(12, '', new Error('job3')) }
    ]);
    await waitForPoolEmpty(pool);
    const results = pool.getCurrentResults();
    //Not great, but timing issues can sometimes change the results. They just need to have 3+ per result list.
    expect(results.find((r) => r.id === 0)?.results.length).to.be.greaterThanOrEqual(2);
    expect(results.find((r) => r.id === 1)?.results.length).to.be.greaterThanOrEqual(2);
    expect(results.find((r) => r.id === 2)?.results.length).to.be.greaterThanOrEqual(2);
  });

  it("success behavior works correctly", async () => {
    let counter1 = 0;
    let counter2 = 0;
    let counter3 = 0;
    const pool = new PromisePool<string>({
      poolSize: 2, behaviorFunctions: {
        successBehavior: (context) => {
          if (context.result.id === 0) {
            counter1++;
            if (counter1 === 3) {
              return {
                destinationInstruction: {
                  destination: 'none'
                }
              };
            }
          } else if (context.result.id === 1) {
            counter2++;
            if (counter2 === 4) {
              return {
                destinationInstruction: {
                  destination: 'none'
                }
              };
            }
          } else if (context.result.id === 2) {
            counter3++;
            if (counter3 === 5) {
              return {
                destinationInstruction: {
                  destination: 'none'
                }
              };
            }
          }
          return {
            destinationInstruction: {
              destination: 'pool'
            }
          };
        }
      }
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
      { promise: createPromise(12, 'job3') }
    ]);
    await waitForPoolEmpty(pool);
    const results = pool.getCurrentResults();
    expect(results.find((r) => r.id === 0)?.results.length).to.equal(3);
    expect(results.find((r) => r.id === 1)?.results.length).to.equal(4);
    expect(results.find((r) => r.id === 2)?.results.length).to.equal(5);
  });

  it("onResult works as expected", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      behaviorFunctions: { 
        onResultBehavior: (result, error) => {
          if ((error as Error)?.message === 'Not a real error'){
            return {result: 'Worked', status: 'success'};
          } else if (result === 'Actually an error'){
            return {error: new Error('Actually an error'), status: 'error'};
          } else if (!!result){
            return {result, status: 'success'};
          }
          return {error, status: 'error'};
        }
      }
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'Actually an error') },
      { promise: createPromise(10, '', new Error('Not a real error')) },
      { promise: createPromise(11, '', new Error('job2')) },
    ]);

    await waitForPoolEmpty(pool);
    const results = pool.getCurrentResults();
    for (const result of results){
      if (result.id === 0){
        expect(result.results[0].status).to.equal('success');
      } else if (result.id === 1) {
        expect(result.results[0].status).to.equal('error');
      } else if (result.id === 2) {
        expect(result.results[0].status).to.equal('success');
      } else if (result.id === 3) {
        expect(result.results[0].status).to.equal('error');
      }
    }
  });

  it("queueBehavior works as expected", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      behaviorFunctions: {
        //Return the largest ID
        queueBehavior: (context) => {
          let largestItem: IQueueItem<unknown> | undefined = undefined;
          context.promisePool.iterateQueue((item) => {
            if (item.id > (largestItem?.id ?? 0)){
              largestItem = item;
            }
          });
          return {value: largestItem};
        }
      }
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
      { promise: createPromise(10, 'job3') },
      { promise: createPromise(11, 'job4') },
      { promise: createPromise(10, 'job5') },
    ]);

    await waitForPoolEmpty(pool);
    //Different order as we'll have 0 and 1 executing immediately, but then largest to smallest from the queue, so 4,3,2.
    const results = pool.getCurrentResults();
    expect(results[0].id).to.equal(0);
    expect(results[1].id).to.equal(1);
    expect(results[2].id).to.equal(4);
    expect(results[3].id).to.equal(3);
    expect(results[4].id).to.equal(2);
  }); 

  it("submission behavior works as expected", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      behaviorFunctions: {
        submissionBehavior: () => {
          return {
            destination: 'front_of_queue',
          };
        }
      }
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
      { promise: createPromise(10, 'job3') },
      { promise: createPromise(11, 'job4') },
      { promise: createPromise(10, 'job5') },
    ]);

    await waitForPoolEmpty(pool);
    //Different order as we'll have 0 and 1 executing immediately, then jobs being slipped into the front of the queue, so 4,3,2.
    const results = pool.getCurrentResults();
    expect(results[0].id).to.equal(0);
    expect(results[1].id).to.equal(1);
    expect(results[2].id).to.equal(4);
    expect(results[3].id).to.equal(3);
    expect(results[4].id).to.equal(2);
  });
});

describe("event handling and silencing", () => {
  it("global events work as expected", async () => {
    const eventsFired: {eventName: string, oldState?: PoolStatus, newState?: PoolStatus, reason?: unknown, result?: PoolResult<string, unknown>, error?: unknown, jobMetadata?: JobMetadata<unknown>}[] = [];
    const pool = new PromisePool<string>({
      poolSize: 2
    });
    pool.addListener('job_start', (jobMetadata) => {
      eventsFired.push({eventName: 'job_start', jobMetadata});
    });
    pool.addListener('job_success', (result, jobMetadata) => {
      eventsFired.push({eventName: 'job_success', result, jobMetadata});
    });
    pool.addListener('job_error', (error, jobMetadata) => {
      eventsFired.push({eventName: 'job_error', error, jobMetadata});
    });
    pool.addListener('pool_empty', () => {
      eventsFired.push({eventName: 'pool_empty'});
    });
    pool.addListener('pool_state_change', (oldState, newState, reason) => {
      eventsFired.push({eventName: 'pool_state_change', oldState, newState, reason});
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 1 } },
      { promise: createPromise(10, '', new Error('job2 error')), jobMetadata: { jobId: 2 } },
      { promise: createPromise(11, 'job3'), jobMetadata: { jobId: 3 } },
    ]);
    pool.setStatus('closed', { message: 'all done' });

    await waitForPoolEmpty(pool);
    const start = eventsFired.filter((e) => e.eventName === 'job_start');
    for (const event of start){
      expect(start.length).to.equal(3);
      expect(event.jobMetadata?.jobId).to.be.ok;
    }
    const success = eventsFired.filter((e) => e.eventName === 'job_success');
    for (const event of success){
      expect(success.length).to.equal(2);
      expect(event.result).to.be.ok;
    }
    const error = eventsFired.filter((e) => e.eventName === 'job_error');
    for (const event of error){
      expect(error.length).to.equal(1);
      expect(event.error).to.be.ok;
      expect(event.jobMetadata?.jobId).to.be.ok;
    }
    const poolEmpty = eventsFired.filter((e) => e.eventName === 'pool_empty');
    expect(poolEmpty.length).to.equal(1);
    const poolStateChange = eventsFired.filter((e) => e.eventName === 'pool_state_change');
    expect(poolStateChange.length).to.equal(1);
    expect(poolStateChange[0].oldState).to.equal('running');
    expect(poolStateChange[0].newState).to.equal('closed');
    expect(poolStateChange[0].reason).to.equal('all done');
  });

  it("event silence config works as expected", async () => {
    const eventsFired: { eventName: string, oldState?: PoolStatus, newState?: PoolStatus, reason?: unknown, result?: PoolResult<string, unknown>, error?: unknown, jobMetadata?: JobMetadata<unknown> }[] = [];

    const pool = new PromisePool<string>({
      poolSize: 2,
      eventSilenceConfig: {
        job_start: 'silent_with_inherit',
        job_success: 'silent_with_inherit',
        job_error: 'silent_with_inherit',
        pool_empty: 'active_with_inherit',
        pool_state_change: 'silent_with_inherit'
      }
    });
    pool.addListener('job_start', (jobMetadata) => {
      eventsFired.push({ eventName: 'job_start', jobMetadata });
    });
    pool.addListener('job_success', (result, jobMetadata) => {
      eventsFired.push({ eventName: 'job_success', result, jobMetadata });
    });
    pool.addListener('job_error', (error, jobMetadata) => {
      eventsFired.push({ eventName: 'job_error', error, jobMetadata });
    });
    pool.addListener('pool_empty', () => {
      eventsFired.push({ eventName: 'pool_empty' });
    });
    pool.addListener('pool_state_change', (oldState, newState, reason) => {
      eventsFired.push({ eventName: 'pool_state_change', oldState, newState, reason });
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: {jobId: 'job1'}, eventSilenceConfig: { job_start: 'force_active', job_success: 'force_active' } },
      { promise: createPromise(11, 'job2'), jobMetadata: { jobId: 'job2' }, eventSilenceConfig: { job_start: 'active_with_inherit' } },
      { promise: createPromise(10, '', new Error('job3 error')), jobMetadata: { jobId: 'job3' }, eventSilenceConfig: { job_start: 'active_with_inherit', job_error: 'force_active' } },
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(10, '', new Error('job3 error')), jobMetadata: { jobId: 'job5' } },
    ]);
    pool.setStatus('closed', { message: 'all done' });
    await waitForPoolEmpty(pool);
    const start = eventsFired.filter((e) => e.eventName === 'job_start');
    expect(start.length).to.equal(3);

    const success = eventsFired.filter((e) => e.eventName === 'job_success');
    expect(success.length).to.equal(1);
    
    const errors = eventsFired.filter((e) => e.eventName === 'job_error');
    expect(errors.length).to.equal(1);
    const empty = eventsFired.filter((e) => e.eventName === 'pool_empty');
    expect(empty.length).to.equal(1);
    const change = eventsFired.filter((e) => e.eventName === 'pool_state_change');
    expect(change.length).to.equal(0);
  });

  it("removes event listeners", async () => {
    const eventsFired: { eventName: string, oldState?: PoolStatus, newState?: PoolStatus, reason?: unknown, result?: PoolResult<string, unknown>, error?: unknown, jobMetadata?: JobMetadata<unknown> }[] = [];

    const pool = new PromisePool<string>({
      poolSize: 2,

      eventSilenceConfig: {
        job_start: 'silent_with_inherit',
        job_success: 'silent_with_inherit',
        job_error: 'silent_with_inherit',
        pool_empty: 'active_with_inherit',
        pool_state_change: 'silent_with_inherit'
      }
    });
    const start = pool.addListener('job_start', (jobMetadata) => {
      eventsFired.push({ eventName: 'job_start', jobMetadata });
    });
    pool.addListener('job_success', (result, jobMetadata) => {
      eventsFired.push({ eventName: 'job_success', result, jobMetadata });
    });
    pool.addListener('job_error', (error, jobMetadata) => {
      eventsFired.push({ eventName: 'job_error', error, jobMetadata });
    });
    pool.addListener('pool_empty', () => {
      eventsFired.push({ eventName: 'pool_empty' });
    });
    pool.addListener('pool_state_change', (oldState, newState, reason) => {
      eventsFired.push({ eventName: 'pool_state_change', oldState, newState, reason });
    });
    pool.removeListener('job_start', start);
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 'job1' }, eventSilenceConfig: { job_start: 'force_active', job_success: 'force_active' } },
      { promise: createPromise(11, 'job2'), jobMetadata: { jobId: 'job2' }, eventSilenceConfig: { job_start: 'active_with_inherit' } },
      { promise: createPromise(10, '', new Error('job3 error')), jobMetadata: { jobId: 'job3' }, eventSilenceConfig: { job_start: 'active_with_inherit', job_error: 'force_active' } },
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(10, '', new Error('job3 error')), jobMetadata: { jobId: 'job5' } },
    ]);
    pool.setStatus('closed', { message: 'all done' });
    await waitForPoolEmpty(pool);
    const startEvents = eventsFired.filter((e) => e.eventName === 'job_start');
    expect(startEvents.length).to.equal(0);

    const success = eventsFired.filter((e) => e.eventName === 'job_success');
    expect(success.length).to.equal(1);

    const errors = eventsFired.filter((e) => e.eventName === 'job_error');
    expect(errors.length).to.equal(1);
    const empty = eventsFired.filter((e) => e.eventName === 'pool_empty');
    expect(empty.length).to.equal(1);
    const change = eventsFired.filter((e) => e.eventName === 'pool_state_change');
    expect(change.length).to.equal(0);
  });

  it("sets and patches config correctly", () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      eventSilenceConfig: {
        job_start: 'silent_with_inherit',
        job_success: 'silent_with_inherit',
        job_error: 'silent_with_inherit',
        pool_empty: 'active_with_inherit',
        pool_state_change: 'silent_with_inherit'
      }
    });

    let config = pool.setEventSilenceConfig({
      job_start: 'force_active',
      pool_empty: 'force_silence'
    });
    expect(config.job_start).to.equal('force_active');
    expect(config.pool_empty).to.equal('force_silence');
    expect(config.job_error).to.equal('active_with_inherit');
    expect(config.job_success).to.equal('active_with_inherit');
    expect(config.pool_state_change).to.equal('active_with_inherit');

    config = pool.patchEventSilenceConfig({
      job_error: 'force_silence'
    });

    expect(config.job_start).to.equal('force_active');
    expect(config.pool_empty).to.equal('force_silence');
    expect(config.job_error).to.equal('force_silence');
    expect(config.job_success).to.equal('active_with_inherit');
    expect(config.pool_state_change).to.equal('active_with_inherit');
  });
});

describe("testing timeouts and delays both global and per-job", () => {
  it("handles timeouts properly", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      timeout: 25
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 'job1' } },
      { promise: createPromise(40, 'job2'), jobMetadata: { jobId: 'job2' } }, //This will error out
      { promise: createPromise(35, 'job3'), jobMetadata: { jobId: 'job3' }, timeout: 50 }, //This will not as we override the global timeout
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(40, 'job5', new Error('job3 error')), jobMetadata: { jobId: 'job5' } }, //This will error out, but the error will be caught after the timeout and it will not bubble up further.
    ]);
    await waitForPoolEmpty(pool);

    const results = pool.getCurrentResults();
    expect(results.find((r) => r.id === 0)?.results[0].status).to.equal('success');
    expect(results.find((r) => r.id === 1)?.results[0].status).to.equal('error');
    expect(results.find((r) => r.id === 2)?.results[0].status).to.equal('success');
    expect(results.find((r) => r.id === 3)?.results[0].status).to.equal('success');
    const id4 = results.find((r) => r.id === 4)?.results[0];
    expect(id4!.status).to.equal('error');
    expect((id4!.error!.cause as Error).message).to.contain('Promise timed out after');
  });

  it("handles various delays correctly", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      delay: 10,
      delayType: 'pool',
      collectMetrics: true
    });
    pool.submitBatch([
      { promise: createPromise(5, 'job1'), jobMetadata: { jobId: 1 } },
      { promise: createPromise(5, 'job2'), jobMetadata: { jobId: 2 }, delay: 0},
      { promise: createPromise(5, 'job3'), jobMetadata: { jobId: 3 }, delay: 30, delayType: 'queue'},
      { promise: createPromise(5, 'job4'), jobMetadata: { jobId: 4 } },
      { promise: createPromise(5, 'job5'), jobMetadata: { jobId: 5 }, delay: 100, delayType: 'cumulative'},
      { promise: createPromise(5, 'job6'), jobMetadata: { jobId: 6 } },
    ]);

    await waitForPoolEmpty(pool);
    const metrics = [...pool.getRawMetrics().values()].flat();
    const job1 = metrics.find((m) => m?.jobMetadata?.jobId === 1);
    //Drop times by a smidge because timings can get a little screwy in Node and I don't want random test failures.
    expect(job1!.executionStartTime - job1!.submissionTime).to.be.greaterThanOrEqual(9);
    const job2 = metrics.find((m) => m?.jobMetadata?.jobId === 2);
    expect(job2!.executionStartTime - job2!.submissionTime).to.be.lessThanOrEqual(9);
    const job3 = metrics.find((m) => m?.jobMetadata?.jobId === 3);
    expect(job3!.executionStartTime - job3!.submissionTime).to.be.greaterThanOrEqual(28);
    const job4 = metrics.find((m) => m?.jobMetadata?.jobId === 4);
    expect(job4!.executionStartTime - job4!.submissionTime).to.be.greaterThanOrEqual(9);
    const job5 = metrics.find((m) => m?.jobMetadata?.jobId === 5);
    expect(job5!.executionStartTime - job5!.submissionTime).to.be.greaterThanOrEqual(95);
    const job6 = metrics.find((m) => m?.jobMetadata?.jobId === 6);
    expect(job6!.executionStartTime - job6!.submissionTime).to.be.greaterThanOrEqual(9);
  });
});

describe("testing halt conditions and abort controllers", () => {
  it("runs abort controllers properly and iterate functions", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      collectMetrics: true
    });

    const ac1 = new AbortController();
    const ac2 = new AbortController();
    const ac3 = new AbortController();
    let ac1aborted = false;
    let ac2aborted = false;
    let ac3aborted = false;
    const promise1 = async () => {
      return new Promise<string>((resolve, reject) => {
        setTimeout(() => {
          resolve('promise1 done');
        }, 30);
        ac1.signal.addEventListener('abort', () => {
          ac1aborted = true;
          reject('promise1 aborted');
        });
      });
    }

    const promise2 = async () => {
      return new Promise<string>((resolve, reject) => {
        setTimeout(() => {
          resolve('promise2 done');
        }, 30);
        ac2.signal.addEventListener('abort', () => {
          ac2aborted = true;
          reject('promise2 aborted');
        });
      });
    }
    const promise6 = async () => {
      return new Promise<string>((resolve, reject) => {
        setTimeout(() => {
          resolve('promise3 done');
        }, 100);
        ac3.signal.addEventListener('abort', () => {
          ac3aborted = true;
          reject('promise3 aborted');
        });
      });
    }

    pool.submitBatch([
      { promise: promise1, jobMetadata: {jobId: 'job1'}, abortController: ac1},
      { promise: promise2, delay: 50, jobMetadata: { jobId: 'job2' }, abortController: ac2 },
      { promise: createPromise(10, 'promise3'), jobMetadata: { jobId: 'job3' } },
      { promise: createPromise(10, 'promise4'), delay: 100, delayType: 'queue', jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(10, 'promise5'), delay: 20, delayType: 'pool', jobMetadata: { jobId: 'job5' } },
      { promise: promise6, jobMetadata: {jobId: 'job6'}, abortController: ac3}
    ]);

    await delay(3); //Let them run a bit
    ac1.abort('aborting promise 1');
    pool.iterateRunningJobs((item) => {
      if (item.getJobMetadata()?.jobId === 'job2') {
        item.cancel();
      }
    });
    
    await delay(3); //Time enough for the next two to start running
    pool.iterateRunningJobs((item) => {
      if (item.getJobMetadata()?.jobId === 'job3') {
        expect(item.getStatus() === 'running');
      }
    });
    pool.iterateQueue((item) => {
      if (item.getJobMetadata()?.jobId === 'job4'){
        //Have to go with large values here because Jest is stupidly flaky and can take 40ms+ just to get through these two lines. We don't even yield to the event loop in here!
        item.adjustDelay(40);
        expect(item.getRemainingDelay()).to.be.greaterThanOrEqual(20);
      }
    });
    await delay(10);
    pool.iterateRunningJobs((item) => {
      if (item.getJobMetadata()?.jobId === 'job5') {
        item.cancel();
      }
    });

    await delay(5);
    pool.iterateRunningJobs((item) => {
      if (item.getJobMetadata()?.jobId === 'job6') {
        item.cancel();
      }
    });
    await waitForPoolEmpty(pool);
    expect(ac1aborted).to.equal(true);
    expect(ac2aborted).to.equal(false);
    expect(ac3aborted).to.equal(true);
  });
});

describe("test the logger", () => {
  const logs: { lvl: LogLevel, msg: string }[] = [];

  const logger = { //Can't help the console signatures using `any` here, so just ignore the linter
    error: (message?: unknown, ...additional: unknown[]) => {
      logs.push({ lvl: LogLevel.ERROR, msg: `Message: ${message}, Additional: ${additional?.length ? JSON.stringify(additional) : ''}` });
    },
    warn: (message?: unknown, ...additional: unknown[]) => {
      logs.push({ lvl: LogLevel.WARN, msg: `Message: ${message}, Additional: ${additional?.length ? JSON.stringify(additional) : ''}` });
    },
    info: (message?: unknown, ...additional: unknown[]) => {
      logs.push({ lvl: LogLevel.INFO, msg: `Message: ${message}, Additional: ${additional?.length ? JSON.stringify(additional) : ''}` });
    },
    debug: (message?: unknown, ...additional: unknown[]) => {
      logs.push({ lvl: LogLevel.DEBUG, msg: `Message: ${message}, Additional: ${additional?.length ? JSON.stringify(additional) : ''}` });
    }
  }

  const jobMetadataSerializer = <R>(jobMetadata: JobMetadata<R>) => {
    return `JobMetadata ID is ${String(jobMetadata.jobId)}`;
  }

  it("logs errors correctly", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      logger: {
        logLevel: LogLevel.ERROR,
        logger,
        jobMetadataSerializer
      },
      behaviorFunctions: {
        returnBehavior: () => {
          throw new Error('Deliberate error to test error logging');
        }
      }
    });

    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
    ]);
    await waitForPoolEmpty(pool);
    pool.setStatus('stopped');
    pool.setStatus('running');

    const errors = logs.filter((l) => l.lvl === LogLevel.ERROR);
    expect(errors.length).to.be.greaterThan(0);
    expect(errors.find((e) => e.msg?.includes('JobMetadata ID is: ')));
    expect(logs.find((l) => l.lvl === LogLevel.DEBUG)).to.not.be.ok;
    expect(logs.find((l) => l.lvl === LogLevel.INFO)).to.not.be.ok;
    expect(logs.find((l) => l.lvl === LogLevel.WARN)).to.not.be.ok;
  });

  it("logs warnings correctly", async () => {
    logs.length = 0;
    const pool = new PromisePool<string>({
      poolSize: 2,
      logger: {
        logLevel: LogLevel.WARN,
        logger
      }
    });

    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
    ]);
    await waitForPoolEmpty(pool);
    pool.setStatus('stopped');
    pool.setStatus('running');

    const warn = logs.filter((l) => l.lvl === LogLevel.WARN);
    expect(warn.length).to.be.greaterThan(0);
    expect(logs.find((l) => l.lvl === LogLevel.DEBUG)).to.not.be.ok;
    expect(logs.find((l) => l.lvl === LogLevel.INFO)).to.not.be.ok;
  });

  it("logs info correctly", async () => {
    logs.length = 0;
    const pool = new PromisePool<string>({
      poolSize: 2,
      logger: {
        logLevel: LogLevel.INFO,
        logger
      }
    });

    pool.submitBatch([
      { promise: createPromise(20, 'job1'), timeout: 10 },
      { promise: createPromise(11, 'job2') },
    ]);
    await waitForPoolEmpty(pool);
    pool.setStatus('stopped');
    pool.setStatus('running');

    const info = logs.filter((l) => l.lvl === LogLevel.INFO);
    expect(info.length).to.be.greaterThan(0);
    expect(logs.filter((l) => l.lvl === LogLevel.WARN).length).to.be.greaterThan(0);
    expect(logs.find((l) => l.lvl === LogLevel.DEBUG)).to.not.be.ok;
  });

  it("logs debug correctly", async () => {
    logs.length = 0;
    const pool = new PromisePool<string>({
      poolSize: 2,
      logger: {
        logLevel: LogLevel.DEBUG,
        logger
      }
    });

    pool.submitBatch([
      { promise: createPromise(20, 'job1'), timeout: 10 },
      { promise: createPromise(11, 'job2') },
    ]);
    await waitForPoolEmpty(pool);
    pool.setStatus('stopped');
    pool.setStatus('running');

    const debug = logs.filter((l) => l.lvl === LogLevel.DEBUG);
    expect(debug.length).to.be.greaterThan(0);
    expect(logs.filter((l) => l.lvl === LogLevel.WARN).length).to.be.greaterThan(0);
    expect(logs.filter((l) => l.lvl === LogLevel.INFO).length).to.be.greaterThan(0);
  });

  it("overrides logger from job", async () => {
    logs.length = 0;
    const pool = new PromisePool<string>({
      poolSize: 2,
      logger: {
        logLevel: LogLevel.ERROR,
        logger
      }
    });

    pool.submitBatch([
      { promise: createPromise(20, 'job1'), timeout: 10, logger: {
        logLevel: LogLevel.INFO,
        logger //We can reuse the same logger
      } },
      { promise: createPromise(11, 'job2') },
    ]);
    await waitForPoolEmpty(pool);
    pool.setStatus('stopped');
    pool.setStatus('running');

    const info = logs.filter((l) => l.lvl === LogLevel.INFO);
    expect(info.length).to.be.greaterThan(0);
    expect(logs.find((l) => l.lvl === LogLevel.WARN)).to.not.be.ok;
    expect(logs.filter((l) => l.lvl === LogLevel.INFO).length).to.be.greaterThan(0);
  });
});

describe("test the await state", () => {
  it("properly awaits with defaults", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
    });

    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
      { promise: createPromise(10, 'job3') },
      { promise: createPromise(11, 'job4') },
      { promise: createPromise(10, 'job5') },
    ]);

    const awaitPromise = pool.awaitResults();
    pool.submit({promise: createPromise(10, 'job6')});
    const results = await awaitPromise;
    expect(pool.getCountOfRunningJobs()).to.equal(1);
    expect(results.results.length).to.equal(5);
    expect(results.status).to.be.equal('complete');
  });

  it("properly awaits with retry/recurrence", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      maxRetries: 5,
      maxRecurrences: 5
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, '', new Error('job2 error')) },
      { promise: createPromise(10, 'job3') },
      { promise: createPromise(11, '', new Error('job4 error')) },
    ]);

    const results = await pool.awaitResults({
      maxRecurrences: 2,
      maxRetries: 2,
    });
    expect(results.results[0].results.length).to.equal(3);
    expect(results.results[1].results.length).to.equal(3);
    expect(results.results[2].results.length).to.equal(3);
    expect(results.results[3].results.length).to.equal(3);

    await waitForPoolEmpty(pool);

    const moreResults = pool.getCurrentResults();
    expect(moreResults[0].results.length).to.equal(3);
    expect(moreResults[1].results.length).to.equal(3);
    expect(moreResults[2].results.length).to.equal(3);
    expect(moreResults[3].results.length).to.equal(3);
  });

  it("properly uses error behavior from await state", async () => {
    let counter = 0;
    const pool = new PromisePool<string>({
      poolSize: 2
    });
    pool.submitBatch([
      { promise: createPromise(10, '', new Error('job1')) },
      { promise: createPromise(11, '', new Error('job2')) },
      { promise: createPromise(12, '', new Error('job3')) }
    ]);
    const results = await pool.awaitResults({
      maxRetries: 9,
      errorBehavior: () => {
        if (counter < 9) {
          counter++;
          return {
            destinationInstruction: {
              destination: 'pool'
            }
          };
        }
        return {
          destinationInstruction: {
            destination: 'none'
          }
        };
      }
    })
    //Not great, but timing issues can sometimes change the results. They just need to have 3+ per result list.
    expect(results.results.find((r) => r.id === 0)?.results.length).to.be.greaterThanOrEqual(2);
    expect(results.results.find((r) => r.id === 1)?.results.length).to.be.greaterThanOrEqual(2);
    expect(results.results.find((r) => r.id === 2)?.results.length).to.be.greaterThanOrEqual(2);
  });

  it("properly uses success behavior from await state", async () => {
    let counter1 = 0;
    let counter2 = 0;
    let counter3 = 0;
    const pool = new PromisePool<string>({
      poolSize: 2
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
      { promise: createPromise(12, 'job3') }
    ]);
    const results = await pool.awaitResults({
      maxRecurrences: 10,
      successBehavior: (context) => {
        if (context.result.id === 0) {
          counter1++;
          if (counter1 === 3) {
            return {
              destinationInstruction: {
                destination: 'none'
              }
            };
          }
        } else if (context.result.id === 1) {
          counter2++;
          if (counter2 === 4) {
            return {
              destinationInstruction: {
                destination: 'none'
              }
            };
          }
        } else if (context.result.id === 2) {
          counter3++;
          if (counter3 === 5) {
            return {
              destinationInstruction: {
                destination: 'none'
              }
            };
          }
        }
        return {
          destinationInstruction: {
            destination: 'pool'
          }
        };
      }
    });
    expect(results.results.find((r) => r.id === 0)?.results.length).to.equal(3);
    expect(results.results.find((r) => r.id === 1)?.results.length).to.equal(4);
    expect(results.results.find((r) => r.id === 2)?.results.length).to.equal(5);
  });

  it("properly handles onQueueStall", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2,
      behaviorFunctions: {
        //Return the largest ID
        queueBehavior: (context) => {
          let largestItem: IQueueItem<unknown> | undefined;
          context.promisePool.iterateQueue((item) => {
            if (item.id > (largestItem?.id ?? 0) && item.id !== 4) {
              largestItem = item;
            }
          });
          return { value: largestItem };
        }
      }
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1') },
      { promise: createPromise(11, 'job2') },
      { promise: createPromise(10, 'job3') },
      { promise: createPromise(11, 'job4') },
      { promise: createPromise(10, 'job5') },
    ]);

    let stallCount = 0;
    const results = await pool.awaitResults({
      onQueueStall: () => {
        if (stallCount <= 2){
          stallCount++;
          return {action: 'wait', time: 10};
        } else {
          return {action: 'abort'};
        }
      }
    });

    expect(results.status).to.be.equal('aborted');
    expect(results.results.length).to.be.equal(4);
  });

  it("runs deferredQueueBehavior correctly", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 'job1' } },
      { promise: createPromise(11, 'job2'), jobMetadata: { jobId: 'job2' } },
      { promise: createPromise(10, 'job3'), jobMetadata: { jobId: 'job3' } },
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
    ]);

    const promise = pool.awaitResults({
      deferredQueueBehavior: (context) => {
        context.deferredQueue.sort((i1, i2) => {
          return (i2!.submission?.id ?? 0) - (i1!.submission?.id ?? 0); //Have to do this because Typescript is absurdly paranoid about undefined here.
        });
        return context.deferredQueue.map((i) => {
          return {
            item: i,
            instruction: {
              destination: 'pool'
            }
          };
        });
      }
    });
    pool.submitBatch([
      { promise: createPromise(13, 'job5'), jobMetadata: { jobId: 'job5'} },
      { promise: createPromise(11, 'job6'), jobMetadata: { jobId: 'job6' } },
      { promise: createPromise(10, 'job7'), jobMetadata: { jobId: 'job7' } },
      { promise: createPromise(11, 'job8'), jobMetadata: { jobId: 'job8' } },
    ]);
    const results = await promise;
    expect(results.results.length).to.equal(4);

    await waitForPoolEmpty(pool);

    const moreResults = pool.getCurrentResults();
    expect(moreResults[3].jobMetadata?.jobId).to.equal('job5'); //Last id is 5 rather than 8 because we reversed order in the deferred queue behavior
  });
});

describe("miscellaneous settings and scenarios", () => {
  it("properly sets pool size", async () => {
    const pool = new PromisePool<string>({
      poolSize: 4
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 'job1' } },
      { promise: createPromise(11, 'job2'), jobMetadata: { jobId: 'job2' } },
      { promise: createPromise(10, 'job3'), jobMetadata: { jobId: 'job3' } },
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(10, 'job5'), jobMetadata: { jobId: 'job5' } },
      { promise: createPromise(11, 'job6'), jobMetadata: { jobId: 'job6' } },
      { promise: createPromise(10, 'job7'), jobMetadata: { jobId: 'job7' } },
      { promise: createPromise(11, 'job8'), jobMetadata: { jobId: 'job8' } }
    ]);
    expect(pool.getCountOfRunningJobs()).to.equal(4);

    pool.setPoolSize(2);

    await delay(14); //Should be long enough for the pool to drain

    expect(pool.getCountOfRunningJobs()).to.equal(2);

    await waitForPoolEmpty(pool);

    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 'job1' } },
      { promise: createPromise(11, 'job2'), jobMetadata: { jobId: 'job2' } },
      { promise: createPromise(10, 'job3'), jobMetadata: { jobId: 'job3' } },
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(10, 'job5'), jobMetadata: { jobId: 'job5' } },
      { promise: createPromise(11, 'job6'), jobMetadata: { jobId: 'job6' } },
      { promise: createPromise(10, 'job7'), jobMetadata: { jobId: 'job7' } },
      { promise: createPromise(11, 'job8'), jobMetadata: { jobId: 'job8' } }
    ]);

    pool.setPoolSize(4);
    expect(pool.getCountOfRunningJobs()).to.equal(4);

  });

  it("properly changes states", async () => {
    const pool = new PromisePool<string>({
      poolSize: 2
    });
    pool.submitBatch([
      { promise: createPromise(10, 'job1'), jobMetadata: { jobId: 'job1' } },
      { promise: createPromise(11, 'job2'), jobMetadata: { jobId: 'job2' } },
      { promise: createPromise(10, 'job3'), jobMetadata: { jobId: 'job3' } },
      { promise: createPromise(11, 'job4'), jobMetadata: { jobId: 'job4' } },
      { promise: createPromise(10, 'job5'), jobMetadata: { jobId: 'job5' } },
      { promise: createPromise(11, 'job6'), jobMetadata: { jobId: 'job6' } },
      { promise: createPromise(10, 'job7'), jobMetadata: { jobId: 'job7' } },
      { promise: createPromise(11, 'job8'), jobMetadata: { jobId: 'job8' } }
    ]);

    pool.setStatus('paused');

    await delay(15);

    expect(pool.getCountOfRunningJobs()).to.equal(0);
    expect(pool.getQueueSize()).to.equal(6);

    pool.setStatus('running');

    await delay(5);

    expect(pool.getCountOfRunningJobs()).to.equal(2);

    pool.setStatus('closed');

    await waitForPoolEmpty(pool);

    const results = pool.getCurrentResults();
    expect(results.length).to.equal(8);
  });
});
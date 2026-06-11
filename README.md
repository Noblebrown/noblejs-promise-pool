<h1>What is noblejs-promise-pool?</h1>

It's promise pool with zero runtime dependencies, an extremely simple base case, and a myriad of configuration options if you need them, such as retries, recurrences, metrics, behaviors, events, and a whole lot more.

<h1>Features</h1>

- A broad array of configuration options, and most can be set globally on the pool, but also overridden on a per-job basis if needed.
- Retries for resubmitting jobs that result in an error
- Recurrences for resubmitting jobs that result in success
- Optional metrics that can be gathered and retained by the pool, but also emitted via a provided handler function, and even support for custom timestamp types
- A number of events as well as a silencing configuration for them
- An array of behavioral functions that can be provided or left at defaults, such as whether or not to return a value from a job, whether a job is a failure or success, what to do on a new submission, how to select the next item from the queue if needed, how to handle retries/recurrences, and more
- Delay configurations to delay items in the queue, the pool, or a combination of the two
- Support for abort controllers for submitted jobs
- Support for a user-provided logger that conforms to `console`
- Timeouts for submitted jobs
- A specific awaiting state that will defer any further submitted jobs until the current submitted jobs are complete and returned
- Optional job metadata with a few optional fields and an optional custom type field if needed.
And much more!

<h1>Getting started</h1>

Configuration is simple. Just instantiate a new pool:

`const pool = new PromisePool({poolSize: 5});`

There are a raft of documented options that can be passed via the pool configuration object. Then you're ready to submit jobs:

`pool.submit({promise: () => {
  return myAsyncFunction();
}});`

Or use `submitBatch` for mutliple submissions. If you want to use the awaiting state, which will complete all jobs in the pool/queue and place any new jobs into a deferred queue until done, just call

`const results = await pool.awaitResults();`

You can optionally pass an object for some additional configuration/overrides in awaitResults.

Have a look at some of the JSDocs and feel free to open an issue if you think they could use some work. Contributions and suggestions are welcome!

<h1>Defaults</h1>

There are a number of different configuration options, but if not provided, they will assume the following defaults:

- Metrics will not be retained and will not be emitted.
- Returned values from jobs will be retained.
- Delays, timeouts, retries, and recurrences will all be 0.
- Waiting jobs in the queue will be selected on a first in, first out basis for any waiting items unless they are delayed, in which case they will be skipped.
- Timestamps will be stored as unix epoch values, ie, `Date.now()` if metrics are retained.
- New submissions will be placed in the pool if there's room, otherwise they will go in the back of the queue.
- All event types will be set to `active_with_inherit`, which will fire events unless explicitly overridden.
- If a promise resolves, it will be flagged as successful. If it rejects, it will be flagged as failed.

If `awaitResults()` is used and no configuration is passed, then the following will be used as defaults:

- If a job is set for retries or recurrences then it will be placed into the deferred queue after completing.
- If the queue stalls (edge case where there are no jobs in the pool, jobs in the queue with no delays, and the queue behavior function does not return any values) then the await state will abort.
- When all jobs are completed, the deferred queue will be drained FIFO into the pool/queue while taking any delays into account.

In addition to these defaults, there are a few premade behaviors that can be passed to the pool to save you the trouble of writing your own:

- `ErrorBehavior.HALT_ON_CONSECUTIVE_RETRY_FAILURE` - Halt the pool if a certain number of retries fail consecutively.
- `ErrorBehavior.HALT_ON_CUMULATIVE_RETRY_FAILURE` - Halt the pool if the total number of retries exceeds a set threshhold.
- `QueueBehavior.PRIORITY` - Relies on the optional `priority` field on submitted jobs and will select the lowest priority job from the queue. If more than one job with that priority exists in the queue, then the frontmost job will be selected.

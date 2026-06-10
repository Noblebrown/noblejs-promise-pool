What is noblejs-promise-pool?

It's promise pool with zero runtime dependencies, an extremely simple base case, and a myriad of configuration options if you need them, such as retries, recurrences, metrics, behaviors, events, and a whole lot more.

Installation is simple:

```bash
npm install noblejs-promise-pool

##Features

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

##Getting started

Configuration is simple. Just instantiate a new pool:

`const pool = new PromisePool({poolSize: 5});`

There are a raft of documented options that can be passed via the pool configuration object. Then you're ready to submit jobs:

`pool.submit({promise: () => {
  return myAsyncFunction();
}});`

Or use `submitBatch` for mutliple submissions. If you want to use the awaiting state, just call

`const results = await pool.awaitResults();`

You can optionally pass an object for some additional configuration/overrides in awaitResults.

Have a look at some of the JSDocs and feel free to open an issue if you think they could use some work. Contributions and suggestions are welcome!
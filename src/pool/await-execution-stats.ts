import { IAwaitExecutionStats } from "../types/behavior";
import { ExecutionStats } from "../types/config";
import type { AwaitContext } from "./contexts";
import type { PromisePool } from "./promise-pool"
/**
 * A class to encapsulate a snapshot of the ExecutionStats at the time `PromisePool.awaitResults()` was called. These stats will not
 * be updated as the pool/queue drains and is used for determining if maximum retries or recurrences have been hit.
 */
export class AwaitExecutionStats<T, R, D> implements IAwaitExecutionStats<T, R, D> {
  /**
   * Constructor that takes a map of pool id -> stats as a snapshot.
   * @param map The execution stats at the time `awaitResults()` was called.
   */
  constructor(private readonly map: Map<number, ExecutionStats>) { }

  /**
   * Gets the {@link ExecutionStats} object for a given pool id, if present
   * @param context The {@link AwaitContext}
   * @returns `ExecutionStats` for the pool id from the `context` or `undefined` if it's not present
   */
  getStats(context: AwaitContext<T, R, D>): ExecutionStats | undefined {
    return this.map.get(context.result.id);
  }

  /**
   * Determine if a given job has exceeded its allowed retries, if any. A falsy value for `context.maxRetries` will always return true
   * @param context The {@link AwaitContext}
   * @returns `true` if exceeded, `false` if not
   */
  exceedsRetries(context: AwaitContext<T, R, D>): boolean {
    const stats = this.getStats(context);
    if (!stats) {
      throw new Error(`Invalid Job ID ${context.result.id} was used to attempt to fetch stats while awaiting results.`);
    }
    return stats.totalRetries + context.maxRetries <= context.result.executionStats.totalRetries;
  }

  /**
   * Determine if a given job has exceeded its allowed recurrences, if any. A falsy value for `context.maxRecurrences` will always return true
   * @param context The {@link AwaitContext}
   * @returns `true` if exceeded, `false` if not
   */
  exceedsRecurrences(context: AwaitContext<T, R, D>): boolean {
    const stats = this.getStats(context);
    if (!stats) {
      throw new Error(`Invalid Job ID ${context.result.id} was used to attempt to fetch stats while awaiting results.`);
    }
    return stats.totalExecutions + context.maxRecurrences < context.result.executionStats.totalExecutions - context.result.executionStats.totalRetries;
  }
}
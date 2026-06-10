export type LoggerFunc = (message?: unknown, ...optionalParams: unknown[]) => void;

/** Interface for a logger. This conforms to `console`, which can be safely passed if desired. */
export interface Logger {
  error: LoggerFunc;
  warn: LoggerFunc;
  debug: LoggerFunc;
  info: LoggerFunc;
}

type RequireLevels<K extends keyof Logger> = Pick<Logger, K> & Partial<Omit<Logger, K>>;

// Requires 'error'. 'warn', 'info', and 'debug' are optional.
export type ErrorLogger = RequireLevels<'error'>;

// Requires 'error' and 'warn'. 'info' and 'debug' are optional.
export type WarnLogger = RequireLevels<'error' | 'warn'>;

// Requires 'error', 'warn', and 'info'. 'debug' is optional.
export type InfoLogger = RequireLevels<'error' | 'warn' | 'info'>;

// Requires all four.
export type DebugLogger = Logger;

/** 
 * Log levels corresponding to each logger function. As with logger systems in general, you get that level + those above, so setting `WARN` will
 * also log `ERROR`.
 */
export enum LogLevel {
  NONE,
  ERROR,
  WARN,
  INFO,
  DEBUG
}
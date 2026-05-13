package com.init;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;

/*
    Generic retry executor with exponential backoff for AWS API calls.
    Wraps any provisioning step so transient failures (throttles, timeouts, 500s)
    are retried before giving up and triggering teardown.
 */
public class RetryExecutor {
    private static final Logger logger = LoggerFactory.getLogger(RetryExecutor.class);

    private static final int DEFAULT_MAX_ATTEMPTS = 3;
    private static final long DEFAULT_BASE_DELAY_MS = 2000;
    private static final long DEFAULT_MAX_DELAY_MS = 30000;

    /**
     * Execute a callable with retry logic and exponential backoff.
     *
     * @param task       The provisioning operation to execute
     * @param stepName   Human-readable name for logging (e.g. "IAM Setup")
     * @param maxAttempts Maximum number of attempts before giving up
     * @param <T>        Return type of the task
     * @return The result of the task if successful
     * @throws RetryExhaustedException if all retry attempts are exhausted
     */
    public static <T> T execute(Callable<T> task, String stepName, int maxAttempts) {
        long delay = DEFAULT_BASE_DELAY_MS;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                logger.info("[{}] Attempt {}/{}", stepName, attempt, maxAttempts);
                T result = task.call();
                logger.info("[{}] Completed successfully on attempt {}", stepName, attempt);
                return result;
            } catch (RuntimeException e) {
                logger.warn("[{}] Attempt {}/{} failed: {}", stepName, attempt, maxAttempts, e.getMessage());

                if (attempt == maxAttempts) {
                    throw new RetryExhaustedException(
                            String.format("[%s] All %d attempts exhausted. Last error: %s",
                                    stepName, maxAttempts, e.getMessage()), e);
                }

                logger.info("[{}] Retrying in {}ms...", stepName, delay);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RetryExhaustedException(
                            String.format("[%s] Retry interrupted", stepName), ie);
                }

                // Exponential backoff with cap
                delay = Math.min(delay * 2, DEFAULT_MAX_DELAY_MS);
            } catch (Exception e) {
                // Checked exceptions that aren't SDK/Runtime — wrap and fail immediately
                throw new RetryExhaustedException(
                        String.format("[%s] Non-retryable exception: %s", stepName, e.getMessage()), e);
            }
        }

        // Unreachable, but satisfies the compiler
        throw new RetryExhaustedException(String.format("[%s] Retry loop exited unexpectedly", stepName), null);
    }

    /**
     * Convenience overload using default max attempts (3).
     */
    public static <T> T execute(Callable<T> task, String stepName) {
        return execute(task, stepName, DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * Execute a Runnable (void return) with retry logic.
     */
    public static void executeVoid(Runnable task, String stepName, int maxAttempts) {
        execute(() -> { task.run(); return null; }, stepName, maxAttempts);
    }

    /**
     * Convenience overload for void tasks with default max attempts.
     */
    public static void executeVoid(Runnable task, String stepName) {
        executeVoid(task, stepName, DEFAULT_MAX_ATTEMPTS);
    }

    /*
        Thrown when all retry attempts are exhausted.
        Signals to the caller that teardown should be initiated.
     */
    public static class RetryExhaustedException extends RuntimeException {
        public RetryExhaustedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

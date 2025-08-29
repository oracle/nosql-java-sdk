package oracle.nosql.driver.util;

import oracle.nosql.driver.NoSQLException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class ConcurrentUtil {
    /**
     * A convenient function to hold the lock and run.
     */
    public static <T> T synchronizedCall(ReentrantLock lock,
                                         Supplier<T> s) {
        lock.lock();
        try {
            return s.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * A convenient function to hold the lock and run.
     */
    public static void synchronizedCall(ReentrantLock lock,
                                        Runnable r) {
        lock.lock();
        try {
            r.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * A helper function to wait for the future to complete.
     */
    public static<T> T awaitFuture(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw ((RuntimeException) cause);
            }
            throw new NoSQLException("ExecutionException: "
                + e.getMessage(), e.getCause());
        } catch (InterruptedException ie) {
            throw new NoSQLException("Request interrupted: "
                + ie.getMessage(), ie);
        }
    }

    /**
     * Returns the cause if the exception is a CompletionException, otherwise
     * returns the exception.
     */
    public static Throwable unwrapCompletionException(Throwable t) {
        Throwable actual = t;
        while (true) {
            if (!(actual instanceof CompletionException)
                    || (actual.getCause() == null)) {
                return actual;
            }
            actual = actual.getCause();
        }
    }
}

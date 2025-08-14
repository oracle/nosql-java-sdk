package oracle.nosql.driver.util;

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
}

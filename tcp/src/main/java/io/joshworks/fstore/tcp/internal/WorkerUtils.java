package io.joshworks.fstore.tcp.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class WorkerUtils {

    private static final Logger logger = LoggerFactory.getLogger(WorkerUtils.class);

    private WorkerUtils() {
    }

    /**
     * Schedules a task for future execution. If the execution is rejected because the worker is shutting
     * down then it is logged at debug level and the exception is not re-thrown
     *
     * @param thread   The IO thread
     * @param task     The task to execute
     * @param timeout  The timeout
     * @param timeUnit The time unit
     */
    public static XnioExecutor.Key executeAfter(XnioIoThread thread, Runnable task, long timeout, TimeUnit timeUnit) {
        try {
            return thread.executeAfter(task, timeout, timeUnit);
        } catch (RejectedExecutionException e) {
            if (thread.getWorker().isShutdown()) {
                logger.warn("Failed to schedule task {} as worker is shutting down", task);
                //we just return a bogus key in this case
                return new XnioExecutor.Key() {
                    @Override
                    public boolean remove() {
                        return false;
                    }
                };
            } else {
                throw e;
            }
        }
    }
}

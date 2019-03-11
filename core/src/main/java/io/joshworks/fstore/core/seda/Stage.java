package io.joshworks.fstore.core.seda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Stage<T> implements Closeable {

    private final Logger logger;

    private final BoundedExecutor executor;
    private final StageHandler<T> handler;
    private final SedaContext sedaContext;
    private final String name;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong correlation = new AtomicLong(Long.MIN_VALUE);

    Stage(String name,
          int corePoolSize,
          int maximumPoolSize,
          int queueSize,
          long keepAliveTime,
          TimeUnit unit,
          boolean blockWhenFull,
          RejectedExecutionHandler rejectionHandler,
          SedaContext sedaContext,
          StageHandler<T> handler) {
        this.logger = LoggerFactory.getLogger(name);
        this.handler = handler;
        this.name = name;
        this.sedaContext = sedaContext;
        SedaThreadPoolExecutor tp = SedaThreadPoolExecutor.create(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, queueSize, rejectionHandler);
        this.executor = new BoundedExecutor(tp, queueSize, blockWhenFull);
    }

    void submit(T event, CompletableFuture<Object> future) {
        if (closed.get()) {
            throw new IllegalStateException("Stage is closed");
        }

        String uuid = name + "_" + correlation.incrementAndGet();
        executor.submitTask(() -> {
            EventContext<T> context = new EventContext<>(uuid, event, sedaContext, future);
            try {
                handler.handle(context);

            } catch (StageHandler.StageHandlerException e) {
                Throwable cause = e.getCause();
                if (cause instanceof EnqueueException) {
                    logger.error("Failed to enqueue event: {}", event);
                } else {
                    logger.error("Failed handling event: " + event, e.getCause());
                }
                future.completeExceptionally(cause);

            } catch (Exception e) {
                logger.error("Failed handling event: " + event, e);
                future.completeExceptionally(e);
            }
        });
    }

    public StageStats stats() {
        return executor.stats();
    }

    public boolean closed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Closing stage");
        executor.shutdown();
    }

    public void close(long timeout, TimeUnit unit) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        close();
        try {
            logger.info("Waiting termination");
            executor.awaitTermination(timeout, unit);
            logger.info("Stage terminated");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void closeNow() {
        List<Runnable> tasks = executor.shutdownNow();
        logger.info("Stopped {} active tasks", tasks.size());
    }

    public String name() {
        return name;
    }

    public static class Builder<T> {

        private int corePoolSize = 1;
        private int maximumPoolSize = 1;
        private int queueSize = 10000;
        private long keepAliveTime = 30000;
        private RejectedExecutionHandler rejectionHandler;
        private boolean blockWhenFull;

        public Builder() {
        }

        public Builder<T> corePoolSize(int corePoolSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("corePoolSize must be greater than zero");
            }
            this.corePoolSize = corePoolSize;
            if (corePoolSize > maximumPoolSize) {
                maximumPoolSize = corePoolSize;
            }
            return this;
        }

        public Builder<T> maximumPoolSize(int maximumPoolSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("maximumPoolSize must be greater than zero");
            }
            this.maximumPoolSize = maximumPoolSize;
            if (corePoolSize > maximumPoolSize) {
                corePoolSize = maximumPoolSize;
            }
            return this;
        }

        public Builder<T> queueSize(int queueSize) {
            if (maximumPoolSize <= 0) {
                throw new IllegalArgumentException("queueSize must be greater than zero");
            }
            this.queueSize = queueSize;
            return this;
        }

        public Builder<T> keepAliveTime(long keepAliveTime) {
            this.keepAliveTime = keepAliveTime;
            return this;
        }

        public Builder<T> blockWhenFull() {
            this.blockWhenFull = true;
            return this;
        }

        public Builder<T> rejectionHandler(RejectedExecutionHandler rejectionHandler) {
            Objects.requireNonNull(rejectionHandler, "Rejection handler must be provided");
            this.rejectionHandler = rejectionHandler;
            return this;
        }

        Stage<T> build(String name, StageHandler<T> handler, SedaContext sedaContext) {
            rejectionHandler = rejectionHandler == null ? new LoggingRejectionHandler(name) : rejectionHandler;
            return new Stage<>(name, corePoolSize, maximumPoolSize, queueSize, keepAliveTime, TimeUnit.MILLISECONDS, blockWhenFull, rejectionHandler, sedaContext, handler);
        }
    }


}

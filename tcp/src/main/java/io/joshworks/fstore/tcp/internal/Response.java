package io.joshworks.fstore.tcp.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Response<T> extends CompletableFuture<T> {

    private enum State {WAITING, DONE, CANCELLED}

    private static final Object POISON_PILL = new Object();
    private final long id;
    private final Consumer<Long> cleaner;
    private State state = State.WAITING;
    private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
    private final long start = System.nanoTime();
    private long end;

    public Response(long id, Consumer<Long> cleaner) {
        this.id = id;
        this.cleaner = cleaner;
    }

    //TODO make package private
    @Override
    public boolean complete(Object response) {
        if (!queue.offer(response)) {
            return false;
        }
        state = State.DONE;
        end = System.nanoTime();
        return true;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        try {
            queue.offer(POISON_PILL);
            state = State.CANCELLED;
            return true;
        } finally {
            cleanUp();
        }
    }

    @Override
    public boolean isCancelled() {
        return State.CANCELLED == state;
    }

    @Override
    public boolean isDone() {
        return State.DONE == state;
    }

    @Override
    public T get() {
        return get(20, TimeUnit.SECONDS);
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        Object response;
        try {
            response = queue.poll(timeout, unit);
            if (response == null) {
                cleanUp();
                throw new RuntimeTimeoutException(unit.toMillis(timeout));
            }
        } catch (InterruptedException e) {
            cleanUp();
            e.printStackTrace();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return getOrThrow(response);
    }


    private T getOrThrow(Object msg) {
        if (msg instanceof ErrorMessage) {
            throw new TcpClientException(((ErrorMessage) msg).message);
        }
        if (msg instanceof NullMessage) {
            return null;
        }
        if (POISON_PILL.equals(msg)) {
            return null;
        }
        return (T) msg;
    }

    public long timeTaken() {
        return (end - start) / 1000;
    }

    private void cleanUp() {
        cleaner.accept(id);
    }
}

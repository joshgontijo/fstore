package io.joshworks.eventry.network.tcp.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Response<T extends Message> implements Future<T> {

    private enum State {WAITING, DONE, CANCELLED}

    private static final Object POISON_PILL = new Object();
    private final long id;
    private final Consumer<Long> cleaner;
    private State state = State.WAITING;
    private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(1);
    private final long start = System.nanoTime();
    private long end;

    Response(long id, Consumer<Long> cleaner) {
        this.id = id;
        this.cleaner = cleaner;
    }

    void complete(Object response) {
        if (!queue.offer(response)) {
            throw new IllegalStateException("Failed to add response to the queue");
        }
        state = State.DONE;
        end = System.nanoTime();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        queue.offer(POISON_PILL);
        state = State.CANCELLED;
        cleanUp();
        return true;
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
        return get(5, TimeUnit.SECONDS);
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        Object response;
        try {
            response = queue.poll(timeout, unit);
            if (response == null) {
                cleanUp();
                return null; //time out
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return getOrThrow(response);
    }


    private T getOrThrow(Object msg) {
        if (msg instanceof ErrorMessage) {
            throw new RuntimeException(((ErrorMessage) msg).message);
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

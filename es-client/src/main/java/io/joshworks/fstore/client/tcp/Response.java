package io.joshworks.fstore.client.tcp;

import io.joshworks.fstore.es.shared.tcp.ErrorMessage;
import io.joshworks.fstore.es.shared.tcp.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Response<T extends Message> implements Future<T> {

    private static enum State {WAITING, DONE, CANCELLED}

    private final long id;
    private final Consumer<Long> cleaner;
    private State state = State.WAITING;
    private final BlockingQueue<Message> queue = new ArrayBlockingQueue<>(1);

    Response(long id, Consumer<Long> cleaner) {
        this.id = id;
        this.cleaner = cleaner;
    }

    void complete(Message response) {
        queue.add(response);
        state = State.DONE;
        cleanUp();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
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
        Message msg;
        try {
            msg = queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return getOrThrow(msg);
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        Message msg;
        try {
            msg = queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (msg == null) {
            cleanUp();
            throw new TimeoutRuntimeException();
        }
        return getOrThrow(msg);
    }

    private T getOrThrow(Message msg) {
        if (msg instanceof ErrorMessage) {
            throw new RuntimeException(((ErrorMessage) msg).message);
        }
        return (T) msg;
    }

    private void cleanUp() {
        cleaner.accept(id);
    }
}

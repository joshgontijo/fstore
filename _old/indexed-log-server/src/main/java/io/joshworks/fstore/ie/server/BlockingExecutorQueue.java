package io.joshworks.fstore.ie.server;

import java.util.concurrent.LinkedBlockingQueue;

public class BlockingExecutorQueue<T> extends LinkedBlockingQueue<T> {

    public BlockingExecutorQueue(int size) {
        super(size);
    }


    @Override
    public boolean offer(T t) {
        try {
            put(t);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return false;
    }

}

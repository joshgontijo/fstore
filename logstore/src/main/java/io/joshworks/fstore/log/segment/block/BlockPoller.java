package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.log.LogPoller;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class BlockPoller<T> implements LogPoller<T> {

    private final LogPoller<Block<T>> delegate;
    private final Queue<T> cached = new LinkedList<>();

    public BlockPoller(LogPoller<Block<T>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T peek() throws InterruptedException {
        T found = cached.peek();
        if(found == null) {
            Block<T> block = delegate.poll();
            if(block != null) {
                cached.addAll(block.entries());
                found = cached.peek();
            }
        }
        return found;
    }

    @Override
    public T poll() throws InterruptedException {
        T found = cached.poll();
        if(found == null) {
            Block<T> block = delegate.poll();
            if(block != null) {
                cached.addAll(block.entries());
                found = cached.poll();
            }

        }
        return found;
    }

    @Override
    public T poll(long limit, TimeUnit timeUnit) throws InterruptedException {
        T found = cached.poll();
        if(found == null) {
            Block<T> block = delegate.poll(limit, timeUnit);
            if(block != null) {
                cached.addAll(block.entries());
                found = cached.poll();
            }
        }
        return found;
    }

    @Override
    public T take() throws InterruptedException {
        T found = cached.poll();
        if(found == null) {
            Block<T> block = delegate.take();
            if(block != null) {
                cached.addAll(block.entries());
                found = cached.poll();
            }
        }
        return found;
    }

    @Override
    public boolean headOfLog() {
        return delegate.headOfLog() && cached.isEmpty();
    }

    @Override
    public boolean endOfLog() {
        return delegate.endOfLog() && cached.isEmpty();
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() throws IOException {
        cached.clear();
        delegate.close();
    }
}

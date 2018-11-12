package io.joshworks.fstore.log;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface LogPoller<T> extends IPosition, Closeable {

    int NO_SLEEP = -1;

    T peek() throws InterruptedException;

    T poll() throws InterruptedException;

    T poll(long limit, TimeUnit timeUnit) throws InterruptedException;

    T take() throws InterruptedException;

    boolean headOfLog();

    boolean endOfLog();

    static <T> LogPoller<T> empty() {
        return new LogPoller<T>() {
            @Override
            public T peek() {
                return null;
            }

            @Override
            public T poll() {
                return null;
            }

            @Override
            public T poll(long limit, TimeUnit timeUnit) {
                return null;
            }

            @Override
            public T take() {
                return null;
            }

            @Override
            public boolean headOfLog() {
                return true;
            }

            @Override
            public boolean endOfLog() {
                return true;
            }

            @Override
            public long position() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                //do nothing
            }
        };
    }

}

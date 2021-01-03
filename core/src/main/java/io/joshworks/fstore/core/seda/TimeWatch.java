package io.joshworks.fstore.core.seda;

import java.util.concurrent.TimeUnit;

public class TimeWatch {
    long start;

    public static TimeWatch start() {
        return new TimeWatch();
    }

    private TimeWatch() {
        reset();
    }

    public TimeWatch reset() {
        start = System.currentTimeMillis();
        return this;
    }

    public long elapsed() {
        return System.currentTimeMillis() - start;
    }

    public long elapsed(TimeUnit unit) {
        return unit.convert(elapsed(), TimeUnit.MILLISECONDS);
    }
}
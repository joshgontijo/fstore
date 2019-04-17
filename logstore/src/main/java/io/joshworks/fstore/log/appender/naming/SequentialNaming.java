package io.joshworks.fstore.log.appender.naming;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class SequentialNaming implements NamingStrategy {

    private final AtomicInteger counter = new AtomicInteger();
    private final int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);

    @Override
    public String prefix() {
        return format("%0" + digits + "d", counter.getAndIncrement());
    }
}

package io.joshworks.fstore.log.appender.naming;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class SequentialNaming implements NamingStrategy {

    final AtomicInteger counter = new AtomicInteger();
    private final int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);

    public SequentialNaming(File root) {
        File[] files = root.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isFile()) {
                int i = tryParseName(file);
                int counterVal = counter.get();
                if (i > counterVal) {
                    counter.set(i);
                }
            }
        }
        if (counter.get() > 0) {
            counter.incrementAndGet();
        }
    }

    private int tryParseName(File file) {
        try {
            String intString = file.getName().split("\\.")[0];
            return Integer.parseInt(intString);
        } catch (NumberFormatException e) {
            System.err.println("Failed to parse: " + e.getMessage());
        }
        return 0;
    }

    @Override
    public String prefix() {
        return format("%0" + digits + "d", counter.getAndIncrement());
    }
}

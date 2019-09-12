package io.joshworks.fstore.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Metrics {

    final Map<String, Long> items;

    public Metrics() {
        this.items = new ConcurrentHashMap<>();
    }

    private Metrics(Map<String, Long> items) {
        this.items = items;
    }

    public void update(String name) {
        update(name, 1);
    }

    public void update(String name, long delta) {
        items.compute(name, (k, v) -> v == null ? delta : v + (delta));
    }

    public void set(String name, long value) {
        items.put(name, value);
    }

    public static Metrics merge(Metrics... items) {
        Map<String, Long> merged = Stream.of(items)
                .flatMap(map -> map.items.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));

        return new Metrics(merged);
    }

}

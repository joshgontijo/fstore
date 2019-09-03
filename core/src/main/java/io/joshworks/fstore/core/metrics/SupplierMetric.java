package io.joshworks.fstore.core.metrics;

import java.util.function.Supplier;

public class SupplierMetric implements Metric {

    private final Supplier<Long> supplier;

    public SupplierMetric(Supplier<Long> supplier) {
        this.supplier = supplier;
    }

    @Override
    public long get() {
        return supplier.get();
    }

    @Override
    public void update(long delta) {
        //do nothing
    }

    @Override
    public void set(long value) {
        //do nothing
    }
}

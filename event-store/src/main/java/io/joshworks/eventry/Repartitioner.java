package io.joshworks.eventry;

import io.joshworks.eventry.log.EventRecord;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Repartitioner implements Runnable {

    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final IEventStore store;
    private final Function<EventRecord, String> partitioner;
    private final StreamIterator source;

    public Repartitioner(IEventStore store, String sourceStream, Function<EventRecord, String> partitioner) {
        this.store = store;
        this.partitioner = partitioner;
        source = store.fromStream(StreamName.of(sourceStream));
        scheduler.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        while (source.hasNext()) {
            EventRecord record = source.next();
            String targetStream = partitioner.apply(record);
            store.linkTo(targetStream, record);
        }
    }

}

package io.joshworks.eventry;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.util.Threads;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class Repartitioner implements Runnable, Closeable {

    private final PartitionedStore store;
    private final String sourceStream;
    private final Function<EventRecord, String> partitioner;
    private final ExecutorService executor;

    private final AtomicBoolean closed = new AtomicBoolean();

    public Repartitioner(PartitionedStore store, String sourceStream, Function<EventRecord, String> partitioner) {
        this.store = store;
        this.sourceStream = sourceStream;
        this.partitioner = partitioner;
        this.executor = Executors.newFixedThreadPool(store.partitions());
    }

    @Override
    public void run() {
        store.forEachPartition(this::runRepartitioning);
        Threads.awaitTerminationOf(executor, 2, TimeUnit.SECONDS, () -> System.out.println("Awaiting repartitioning tasks to complete"));
    }

    private void runRepartitioning(IEventStore store) {

        executor.execute(() -> {
            EventStoreIterator streamIt = store.fromStreams(sourceStream);
            while (!closed.get()) {
                while (!streamIt.hasNext()) {
                    Threads.sleep(1000);
                }
                EventRecord record = streamIt.next();
                String targetStream = partitioner.apply(record);
                store.linkTo(targetStream, record);
            }
        });
    }

    @Override
    public void close() {
        closed.set(true);
    }
}

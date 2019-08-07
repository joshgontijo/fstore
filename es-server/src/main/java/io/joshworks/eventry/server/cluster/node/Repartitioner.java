package io.joshworks.eventry.server.cluster.node;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.util.Threads;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Repartitioner implements Runnable, Closeable {

    private final PartitionedStore store;
    private final String sourceStream;
    private final Function<EventRecord, String> partitioner;
    private final ExecutorService executor;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final Map<String, EventStoreIterator> checkpoints = new ConcurrentHashMap<>();

    public Repartitioner(PartitionedStore store, String sourceStream, Function<EventRecord, String> partitioner) {
        this.store = store;
        this.sourceStream = sourceStream;
        this.partitioner = partitioner;
        this.executor = Executors.newFixedThreadPool(store.numPartitions());
    }

    @Override
    public void run() {
        store.forEachPartition(this::runRepartitioning);
    }

    private void runRepartitioning(Node node) {
        executor.execute(() -> {
            IEventStore store = node.store();
            EventStoreIterator streamIt = store.fromStreams(EventMap.from(EventId.of(sourceStream)));
            checkpoints.put(node.id(), streamIt);
            while (!closed.get()) {
                while (!streamIt.hasNext()) {
                    if (!closed.get()) {
                        return;
                    }
                    Threads.sleep(1000);
                }
                EventRecord record = streamIt.next();
                String targetStream = partitioner.apply(record);
                store.linkTo(targetStream, record);
            }
        });
    }

    public Map<String, EventMap> stats() {
        return checkpoints.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().checkpoint()));
    }

    @Override
    public void close() {
        closed.set(true);
        Threads.awaitTerminationOf(executor, 2, TimeUnit.SECONDS, () -> System.out.println("Awaiting repartitioning tasks to complete"));
    }
}

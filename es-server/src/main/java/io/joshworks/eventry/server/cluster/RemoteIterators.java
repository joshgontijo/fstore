package io.joshworks.eventry.server.cluster;

import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.core.io.IOUtils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RemoteIterators implements Closeable {

    public static final int DEFAULT_BATCH_SIZE = 20;
    public static final int DEFAULT_TIMEOUT = 10; //seconds

    private final Map<String, TimestampedIterator> items = new ConcurrentHashMap<>();

    public String add(long timeout, int batchSize, EventStoreIterator delegate) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);

        TimestampedIterator timestamped = new TimestampedIterator(timeout, batchSize, delegate);

        items.put(uuid, timestamped);
        return uuid;
    }

    public Optional<EventStoreIterator> get(String uuid) {
        return Optional.ofNullable(items.get(uuid));
    }

    public List<EventRecord> nextBatch(String uuid) {
        TimestampedIterator it = items.get(uuid);
        if (it == null) {
            throw new IllegalArgumentException("No Remote iterator for " + uuid);
        }
        List<EventRecord> records = new ArrayList<>();
        int read = 0;
        while (it.hasNext() && read++ < it.batchSize) {
            records.add(it.next());
        }
        return records;
    }

    @Override
    public void close() {
        items.values().forEach(IOUtils::closeQuietly);
        items.clear();
    }


    private static class TimestampedIterator implements EventStoreIterator {
        private final long created;
        private final long timeout;
        private final int batchSize;
        private long lastRead;
        private final EventStoreIterator iterator;

        public TimestampedIterator(long timeout, int batchSize, EventStoreIterator iterator) {
            this.timeout = timeout;
            this.batchSize = batchSize;
            this.created = System.currentTimeMillis();
            this.iterator = iterator;
        }

        @Override
        public void close() {
            iterator.close();
        }

        @Override
        public boolean hasNext() {
            lastRead = System.currentTimeMillis();
            return iterator.hasNext();
        }

        @Override
        public EventRecord next() {
            lastRead = System.currentTimeMillis();
            return iterator.next();
        }

        @Override
        public EventMap checkpoint() {
            throw new UnsupportedOperationException("TODO");
        }
    }

}

package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class RemoteIterators {

    private final Map<String, LogIterator<EventRecord>> items = new ConcurrentHashMap<>();

    public String add(long timeout, int batchSize, LogIterator<EventRecord> delegate) {
        String uuid = UUID.randomUUID().toString().substring(0, 8);

        TimestampedIterator timestamped = new TimestampedIterator(timeout, batchSize, delegate);

        items.put(uuid, timestamped);
        return uuid;
    }

    public Optional<LogIterator<EventRecord>> get(String uuid) {
        return Optional.ofNullable(items.get(uuid));
    }


    private static class TimestampedIterator implements LogIterator<EventRecord> {
        private final long created;
        private final long timeout;
        private final int batchSize;
        private long lastRead;
        private final LogIterator<EventRecord> iterator;

        public TimestampedIterator(long timeout, int batchSize, LogIterator<EventRecord> iterator) {
            this.timeout = timeout;
            this.batchSize = batchSize;
            this.created = System.currentTimeMillis();
            this.iterator = iterator;
        }

        @Override
        public long position() {
            lastRead = System.currentTimeMillis();
            return iterator.position();
        }

        @Override
        public void close() throws IOException {
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
    }

}

package io.joshworks.eventry.log;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.Config;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PartitionedEventLog implements IEventLog {

    private static final int PARTITION_BITS = 3; //up to 8 logs per store

    private final IEventLog[] partitions;

    public PartitionedEventLog(int numPartitions, Config<EventRecord> config) {
        if (numPartitions > 1 << PARTITION_BITS) {

        }
        this.partitions = new IEventLog[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new EventLog(config);
        }
    }

    private IEventLog select(long hash) {
        if (partitions.length == 1) {
            return partitions[0];
        }
        return partitions[(int) (hash % partitions.length)];
    }

    @Override
    public long append(EventRecord event) {
        return select(event.hash()).append(event);
    }

    @Override
    public EventRecord get(long position) {
        int logIdx = (int) (position >>> PARTITION_BITS);

        long mask = (1L << PARTITION_BITS) - 1;
        long logPos = (position & mask);
        return partitions[logIdx].get(logPos);
    }

    @Override
    public long entries() {
        return Arrays.stream(partitions).mapToLong(IEventLog::entries).sum();
    }

    @Override
    public void close() {
        for (IEventLog store : partitions) {
            store.close();
        }
    }

    @Override
    public EventStoreIterator iterator(Direction direction) {
        List<EventStoreIterator> iterators = Arrays.stream(partitions).map(l -> l.iterator(direction)).collect(Collectors.toList());
        return new OrderedIterator(iterators);
    }

    @Override
    public EventStoreIterator iterator(Direction direction, EventMap checkpoint) {
        for (Long position : checkpoint.streamHashes()) {

        }


        List<EventStoreIterator> iterators = Arrays.stream(partitions).map(l -> l.iterator(direction)).collect(Collectors.toList());
        return new OrderedIterator(iterators);
    }

    @Override
    public void compact() {

    }

    private static class OrderedIterator implements EventStoreIterator {

        private final List<PeekingIterator<EventRecord>> iterators;
        private final Collection<EventStoreIterator> originals;

        OrderedIterator(Collection<EventStoreIterator> iterators) {
            this.originals = iterators;
            this.iterators = iterators.stream().map(PeekingIterator::new).collect(Collectors.toList());
        }

        @Override
        public boolean hasNext() {
            for (PeekingIterator<EventRecord> next : iterators) {
                if (next.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public EventRecord next() {
            if (iterators.isEmpty()) {
                throw new NoSuchElementException();
            }
            Iterator<PeekingIterator<EventRecord>> itit = iterators.iterator();
            PeekingIterator<EventRecord> prev = null;
            while (itit.hasNext()) {
                PeekingIterator<EventRecord> curr = itit.next();
                if (!curr.hasNext()) {
                    itit.remove();
                    continue;
                }
                if (prev == null) {
                    prev = curr;
                    continue;
                }

                int c = Long.compare(prev.peek().timestamp, curr.peek().timestamp);
                prev = c >= 0 ? curr : prev;
            }
            if (prev != null) {
                return prev.next();
            }
            return null;
        }

        @Override
        public void close() {
            iterators.forEach(IOUtils::closeQuietly);
        }

        @Override
        public EventMap checkpoint() {
            return originals.stream()
                    .map(EventStoreIterator::checkpoint)
                    .reduce(EventMap.empty(), EventMap::merge);
        }
    }

}

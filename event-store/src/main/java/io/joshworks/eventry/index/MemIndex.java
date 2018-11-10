package io.joshworks.eventry.index;


import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MemIndex implements Index {

    private final Map<Long, List<IndexEntry>> index = new ConcurrentHashMap<>();
    private final List<IndexEntry> insertOrder = new ArrayList<>();
    private final AtomicInteger size = new AtomicInteger();

    private final List<MemPoller> pollers = new ArrayList<>();

    public void add(IndexEntry entry) {
        index.compute(entry.stream, (k, v) -> {
            if (v == null)
                v = new ArrayList<>();
            v.add(entry);
            size.incrementAndGet();
            insertOrder.add(entry);
//            adToPollers(entry);
            return v;
        });
    }

    @Override
    public int version(long stream) {
        List<IndexEntry> entries = index.get(stream);
        if (entries == null) {
            return IndexEntry.NO_VERSION;
        }
        synchronized (entries) {
            return entries.get(entries.size() - 1).version;
        }
    }

    public int size() {
        return size.get();
    }


    public boolean isEmpty() {
        return size.get() == 0;
    }

    @Override
    public void close() {
        synchronized (pollers) {
            for (MemPoller poller : pollers) {
                poller.close();
            }
        }
    }

    @Override
    public LogIterator<IndexEntry> iterator(Direction direction, Range range) {
        List<IndexEntry> entries = index.get(range.stream);
        if (entries == null || entries.isEmpty()) {
            return Iterators.empty();
        }
        synchronized (entries) {
            List<IndexEntry> copy = List.copyOf(entries);
            LogIterator<IndexEntry> entriesIt = Direction.BACKWARD.equals(direction) ? Iterators.reversed(copy) : Iterators.of(copy);
            return Iterators.filtering(entriesIt, ie -> ie.version >= range.startVersionInclusive && ie.version < range.endVersionExclusive);
        }
    }

    @Override
    public Stream<IndexEntry> stream(Direction direction, Range range) {
        return Iterators.closeableStream(iterator(direction, range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        List<IndexEntry> entries = index.get(stream);
        if (entries == null || entries.isEmpty()) {
            return Optional.empty();
        }

        synchronized (entries) {
            int idx = Collections.binarySearch(entries, IndexEntry.of(stream, version, 0));
            if (idx >= 0) {
                return Optional.of(entries.get(idx));
            }
            return Optional.empty();
        }
    }

    public List<IndexEntry> getAllOf(long stream) {
        List<IndexEntry> entries = index.get(stream);
        if(entries == null) {
            return new ArrayList<>();
        }
        synchronized (entries) {
            return new ArrayList<>(entries);
        }
    }

    public Iterator<IndexEntry> iterator() {
        var copy = new HashSet<>(index.entrySet()); //sorted is a stateful operation
        List<IndexEntry> ordered = copy.stream()
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());

        return Iterators.of(ordered);
    }

    PollingSubscriber<IndexEntry> poller(List<Range> ranges) {
        synchronized (pollers) {
            MemPoller memPoller = new MemPoller(ranges);
            pollers.add(memPoller);
            return memPoller;
        }
    }

    private class MemPoller implements PollingSubscriber<IndexEntry> {

        private static final int VERIFICATION_INTERVAL_MILLIS = 500;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int position = 0;

        private final Map<Long, Range> ranges;

        private MemPoller(List<Range> ranges) {
            this.ranges = ranges.stream().collect(Collectors.toMap(r -> r.stream, Function.identity()));
        }

        private boolean hasData() {
            return position < insertOrder.size();
        }

        private void waitFor(long time, TimeUnit timeUnit) throws InterruptedException {
            if (time < 0) {
                return;
            }
            long elapsed = 0;
            long start = System.currentTimeMillis();
            long maxWaitTime = timeUnit.toMillis(time);
            long interval = Math.min(maxWaitTime, VERIFICATION_INTERVAL_MILLIS);
            while (!closed.get() && !hasData() && elapsed < maxWaitTime) {
                TimeUnit.MILLISECONDS.sleep(interval);
                elapsed = System.currentTimeMillis() - start;
            }
        }

        private boolean matchRange(IndexEntry indexEntry) {
            if (indexEntry == null || !ranges.containsKey(indexEntry.stream)) {
                return false;
            }
            return ranges.get(indexEntry.stream).match(indexEntry);
        }

        private IndexEntry getEntry() {
            IndexEntry indexEntry = null;
            while (hasData() && !matchRange(indexEntry)) {
                indexEntry = insertOrder.get(position);
                if (!matchRange(indexEntry)) {
                    position++;
                }
            }
            return matchRange(indexEntry) ? indexEntry : null;
        }

        @Override
        public synchronized IndexEntry peek() {
            return getEntry();
        }

        @Override
        public synchronized IndexEntry poll() throws InterruptedException {
            return poll(-1, TimeUnit.SECONDS);
        }

        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            IndexEntry indexEntry = getEntry();
            if(indexEntry == null) {
                waitFor(limit, timeUnit);
            }
            if (indexEntry == null && hasData()) {
                indexEntry = getEntry();
                if (indexEntry != null) {
                    position++;
                }
            }
            if(indexEntry != null) {
                position++;
            }
            return indexEntry;
        }

        @Override
        public synchronized IndexEntry take() {
           throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean headOfLog() {
            return !hasData();
        }

        @Override
        public synchronized boolean endOfLog() {
            return closed.get() && !hasData();
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void close() {
            closed.set(true);
        }

    }

}
package io.joshworks.eventry.index;


import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
        for (MemPoller poller : new ArrayList<>(pollers)) {
            poller.close();
        }
    }

    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction) {
        var copy = new HashSet<>(index.entrySet()); //sorted is a stateful operation
        List<IndexEntry> ordered = copy.stream()
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());

        return Iterators.of(ordered);
    }

    @Override
    public LogIterator<IndexEntry> indexIterator(Direction direction, Range range) {
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
    public Stream<IndexEntry> indexStream(Direction direction) {
        return Iterators.closeableStream(indexIterator(direction));
    }

    @Override
    public Stream<IndexEntry> indexStream(Direction direction, Range range) {
        return Iterators.closeableStream(indexIterator(direction, range));
    }

    @Override
    public Optional<IndexEntry> get(long stream, int version) {
        List<IndexEntry> entries = index.get(stream);
        if (entries == null || entries.isEmpty()) {
            return Optional.empty();
        }

        synchronized (entries) {
            int idx = Collections.binarySearch(entries, IndexEntry.of(stream, version, 0));
            if(idx >= 0) {
                return Optional.of(entries.get(idx));
            }
            return Optional.empty();
        }
    }

    PollingSubscriber<IndexEntry> poller() {
        MemPoller memPoller = new MemPoller();
        pollers.add(memPoller);
        return memPoller;
    }

    private class MemPoller implements PollingSubscriber<IndexEntry> {

        private static final int VERIFICATION_INTERVAL_MILLIS = 500;
        private final AtomicBoolean closed = new AtomicBoolean();
        private int position = 0;

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

        private void waitForData(long time, TimeUnit timeUnit) throws InterruptedException {
            while (!closed.get() && !hasData()) {
                timeUnit.sleep(time);
            }
        }

        @Override
        public synchronized IndexEntry peek() {
            if (hasData()) {
                return insertOrder.get(position);
            }
            return null;
        }

        @Override
        public synchronized IndexEntry poll() throws InterruptedException {
            return poll(-1, TimeUnit.SECONDS);
        }

        @Override
        public synchronized IndexEntry poll(long limit, TimeUnit timeUnit) throws InterruptedException {
            if (hasData()) {
                return insertOrder.get(position++);
            }
            waitFor(limit, timeUnit);
            if (hasData()) {
                return insertOrder.get(position++);
            }
            return null;
        }

        @Override
        public synchronized IndexEntry take() throws InterruptedException {
            if (hasData()) {
                return insertOrder.get(position++);
            }
            waitForData(VERIFICATION_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            if (hasData()) {
                return insertOrder.get(position++);
            }
            //poller was closed while waiting for data
            return null; //TODO shouldn't be an InterruptedException ?
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
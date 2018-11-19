package io.joshworks.eventry.index;


import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MemIndex implements Closeable {

    private final Map<Long, List<IndexEntry>> index = new ConcurrentHashMap<>();
    private final List<IndexEntry> insertOrder = new ArrayList<>();
    private final AtomicInteger size = new AtomicInteger();

    private final List<MemIterator> pollers = new ArrayList<>();

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

    public void close() {
        synchronized (pollers) {
            for (MemIterator poller : pollers) {
                poller.close();
            }
        }
    }

    public LogIterator<IndexEntry> indexedIterator(Direction direction, Range range) {
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

    public LogIterator<IndexEntry> iterator() {
        var copy = new HashSet<>(index.entrySet()); //sorted is a stateful operation
        List<IndexEntry> ordered = copy.stream()
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());

        return Iterators.of(ordered);
    }


    private class MemIterator implements LogIterator<IndexEntry> {

        private final AtomicBoolean closed = new AtomicBoolean();
        private int position = 0;

        private final Map<Long, Range> ranges;

        private MemIterator(List<Range> ranges) {
            this.ranges = ranges.stream().collect(Collectors.toMap(r -> r.stream, Function.identity()));
        }

        private boolean hasData() {
            return position < insertOrder.size();
        }

        private boolean matchRange(IndexEntry indexEntry) {
            if (indexEntry == null || !ranges.containsKey(indexEntry.stream)) {
                return false;
            }
            return ranges.get(indexEntry.stream).match(indexEntry);
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public IndexEntry next() {
            IndexEntry indexEntry = null;
            while (hasData() && !matchRange(indexEntry)) {
                indexEntry = insertOrder.get(position);
                if (!matchRange(indexEntry)) {
                    position++;
                }
            }
            indexEntry =  matchRange(indexEntry) ? indexEntry : null;

            if(indexEntry != null) {
                position++;
            }
            return indexEntry;
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
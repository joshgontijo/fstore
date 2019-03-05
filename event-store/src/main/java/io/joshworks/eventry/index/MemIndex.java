package io.joshworks.eventry.index;


import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.LogIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MemIndex {

    private AtomicInteger size = new AtomicInteger();
    private final Map<Long, List<IndexEntry>> index = new TreeMap<>();

    public synchronized void add(IndexEntry entry) {
        index.compute(entry.stream, (k, v) -> {
            if (v == null)
                v = new ArrayList<>();
            v.add(entry);
            return v;
        });
        size.incrementAndGet();
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
        return size() == 0;
    }

    public synchronized LogIterator<IndexEntry> indexedIterator(Direction direction, Range range) {
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

    public LogIterator<IndexEntry> iterator() {
        var copy = new HashSet<>(index.entrySet()); //iterator is a stateful operation
        List<IndexEntry> ordered = copy.stream()
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());

        return Iterators.of(ordered);
    }


}
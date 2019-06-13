package io.joshworks.eventry.index;


import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MemIndex {

    private AtomicInteger size = new AtomicInteger();
    private final SortedMap<Long, List<IndexEntry>> index = new TreeMap<>();

    public synchronized void add(IndexEntry entry) {
        index.compute(entry.stream, (k, v) -> {
            if (v == null)
                v = new ArrayList<>();
            synchronized (v) {
                v.add(entry);
            }
            return v;
        });
        size.incrementAndGet();
    }

    public int version(long stream) {
        List<IndexEntry> entries = index.get(stream);
        if (entries == null) {
            return EventRecord.NO_VERSION;
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
            List<IndexEntry> sliced = slice(entries, range);
            LogIterator<IndexEntry> entriesIt = Direction.BACKWARD.equals(direction) ? Iterators.reversed(sliced) : Iterators.of(sliced);
            return Iterators.filtering(entriesIt, inRange(range));
        }
    }

    //optimized to avoid reiterating over all entries in the mem list
    private List<IndexEntry> slice(List<IndexEntry> entries, Range range) {
        IndexEntry start = range.start();
        int idx = Collections.binarySearch(entries, start);
        if (idx < 0) { //not found in mem
            return new ArrayList<>();
        }
        int lastIdx = idx + (range.end().version - start.version);
        int endIdx = Math.min(entries.size(), lastIdx);
        List<IndexEntry> subList = List.copyOf(entries.subList(idx, endIdx));
        return Collections.unmodifiableList(subList);
    }

    private Predicate<IndexEntry> inRange(Range range) { //safe guard
        return ie -> ie.version >= range.startVersionInclusive && ie.version < range.endVersionExclusive;
    }

    public Optional<IndexEntry> get(long stream, int version) {
        List<IndexEntry> entries = index.get(stream);
        if (entries == null || entries.isEmpty()) {
            return Optional.empty();
        }

        synchronized (entries) {
            int firstVersion = entries.get(0).version;
            int lastVersion = firstVersion + entries.size() - 1;
            if (version > lastVersion || version < firstVersion) {
                return Optional.empty();
            }
            return Optional.of(entries.get(version - firstVersion));
        }
    }

    public LogIterator<IndexEntry> iterator() {
        var copy = new HashSet<>(index.entrySet()); //iterator is a stateful operation here
        List<IndexEntry> ordered = copy.stream()
                .map(Map.Entry::getValue)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());

        return Iterators.of(ordered);
    }


}
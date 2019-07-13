package io.joshworks.eventry.index;


import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;

public class MemIndex {

    //    private final SortedMap<Long, List<IndexEntry>> index = new TreeMap<>();
    private final ConcurrentSkipListSet<IndexEntry> index = new ConcurrentSkipListSet<>();

    public synchronized void add(IndexEntry entry) {
        index.add(entry);
    }

    public int version(long stream) {
        IndexEntry found = index.floor(IndexEntry.of(stream, Integer.MAX_VALUE, -1));
        if (found == null || found.stream != stream) {
            return EventRecord.NO_VERSION;
        }
        return found.version;
    }

    public int size() {
        return index.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public synchronized LogIterator<IndexEntry> indexedIterator(Direction direction, Range range) {
        NavigableSet<IndexEntry> sliced = index.subSet(range.start(), range.end());
        LogIterator<IndexEntry> entriesIt = Direction.BACKWARD.equals(direction) ? Iterators.wrap(sliced.descendingIterator()) : Iterators.of(sliced);
        return Iterators.filtering(entriesIt, inRange(range));
    }

    private Predicate<IndexEntry> inRange(Range range) { //safe guard
        return ie -> ie.version >= range.startVersionInclusive && ie.version < range.endVersionExclusive;
    }

    public Optional<IndexEntry> get(long stream, int version) {
        IndexEntry expected = IndexEntry.of(stream, version, -1);
        IndexEntry found = index.floor(expected);
        if (!expected.equals(found)) {
            return Optional.empty();
        }
        return Optional.of(found);
    }

    public LogIterator<IndexEntry> iterator() {
        return Iterators.of(index);
    }


}
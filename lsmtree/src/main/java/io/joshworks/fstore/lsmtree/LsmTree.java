package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.lsmtree.log.Record;
import io.joshworks.fstore.lsmtree.log.TransactionLog;
import io.joshworks.fstore.lsmtree.mem.MemTable;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;
    private final MemTable<K, V> memTable;
    private final int flushThreshold;

    //TODO expose logstore parameters
    private LsmTree(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold, String name) {
        this.flushThreshold = flushThreshold;
        this.sstables = new SSTables<>(dir, keySerializer, valueSerializer, flushThreshold, name);
        this.log = new TransactionLog<>(dir, keySerializer, valueSerializer, name);
        this.memTable = new MemTable<>();
        this.log.restore(this::restore);
    }

    public static <K extends Comparable<K>, V> LsmTree<K, V> open(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold) {
        return open(dir, keySerializer, valueSerializer, flushThreshold, "lsm-tree");
    }

    public static <K extends Comparable<K>, V> LsmTree<K, V> open(File dir, Serializer<K> keySerializer, Serializer<V> valueSerializer, int flushThreshold, String name) {
        return new LsmTree<>(dir, keySerializer, valueSerializer, flushThreshold, name);
    }

    public synchronized void put(K key, V value) {
        log.append(Record.add(key, value));
        if (memTable.size() >= flushThreshold) {
            flushMemTable();
        }
        memTable.add(key, value);
    }

    public synchronized V get(K key) {
        V found = memTable.get(key);
        if (found != null) {
            return found;
        }

        return sstables.getByKey(key);
    }

    public V remove(K key) {
        V deleted = memTable.delete(key);
        if (deleted != null) {
            return deleted;
        }
        V found = get(key);
        if (found == null) {
            return null;
        }
        log.append(Record.delete(key));
        return found;
    }

    public CloseableIterator<Entry<K, V>> iterator() {
        List<LogIterator<Entry<K, V>>> segmentsIterators = sstables.segmentsIterator();
        Collection<Entry<K, V>> memItems = memTable.copy().values();
        return new LsmTreeIterator<>(segmentsIterators, memItems);
    }

    public Stream<Entry<K, V>> stream() {
        return Iterators.closeableStream(iterator());
    }


    @Override
    public void close() {
        sstables.close();
        log.close();
    }

    private synchronized void flushMemTable() {
        if (memTable.size() < flushThreshold) {
            return;
        }

        while (!memTable.isEmpty()) {
            Iterator<Map.Entry<K, Entry<K, V>>> iterator = memTable.iterator();
            long lastPos = 0;
            while (iterator.hasNext() && lastPos != Storage.EOF) {
                Map.Entry<K, Entry<K, V>> entry = iterator.next();
                lastPos = sstables.write(entry.getValue());
                if (lastPos != Storage.EOF) {
                    iterator.remove();
                }
            }
            sstables.roll();
        }
        log.markFlushed();

    }

    private void restore(Record<K, V> record) {
        if (EntryType.ADD.equals(record.type)) {
            memTable.add(record.key, record.value);
        }
        if (EntryType.DELETE.equals(record.type)) {
            memTable.delete(record.key);
        }
    }

    private static class LsmTreeIterator<K extends Comparable<K>, V> implements CloseableIterator<Entry<K, V>> {

        private final List<Iterators.PeekingIterator<Entry<K, V>>> segments;

        private LsmTreeIterator(List<LogIterator<Entry<K, V>>> segmentsIterators, Collection<Entry<K, V>> memItems) {
            LogIterator<Entry<K, V>> memIterator = Iterators.of(memItems);
            segmentsIterators.add(memIterator);

            this.segments = segmentsIterators.stream()
                    .map(Iterators::peekingIterator)
                    .collect(Collectors.toList());

            removeSegmentIfCompleted();
        }

        @Override
        public Entry<K, V> next() {
            Entry<K, V> entry;
            do {
                entry = getNextEntry(segments);
            } while (entry != null && hasNext() && !EntryType.ADD.equals(entry.type));
            removeSegmentIfCompleted();
            if (entry == null) {
                throw new NoSuchElementException();
            }
            return entry;
        }

        private void removeSegmentIfCompleted() {
            Iterator<Iterators.PeekingIterator<Entry<K, V>>> itit = segments.iterator();
            while (itit.hasNext()) {
                Iterators.PeekingIterator<Entry<K, V>> seg = itit.next();
                if (!seg.hasNext()) {
                    IOUtils.closeQuietly(seg);
                    itit.remove();
                }
            }
        }

        @Override
        public void close() throws IOException {
            for (Iterators.PeekingIterator<Entry<K, V>> availableSegment : segments) {
                availableSegment.close();
            }
        }

        @Override
        public boolean hasNext() {
            return !segments.isEmpty();
        }

        private Entry<K, V> getNextEntry(List<Iterators.PeekingIterator<Entry<K, V>>> segmentIterators) {
            if (!segmentIterators.isEmpty()) {
                Iterators.PeekingIterator<Entry<K, V>> prev = null;
                for (Iterators.PeekingIterator<Entry<K, V>> curr : segmentIterators) {
                    if (!curr.hasNext()) {
                        continue; //will be removed afterwards
                    }
                    if (prev == null) {
                        prev = curr;
                        continue;
                    }
                    Entry<K, V> prevItem = prev.peek();
                    Entry<K, V> currItem = curr.peek();
                    int c = prevItem.compareTo(currItem);
                    if (c == 0) { //duplicate remove from oldest entry
                        prev.next();
                    }
                    if (c >= 0) {
                        prev = curr;
                    }
                }
                if (prev != null) {
                    return prev.next();
                }
            }
            return null;
        }
    }
}

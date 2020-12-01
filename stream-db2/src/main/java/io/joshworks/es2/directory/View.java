package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class View<T extends SegmentFile> implements Closeable, Iterable<T> {

    private final AtomicInteger refCounter = new AtomicInteger();
    private final Set<T> markedForDeletion = new HashSet<>();
    private final Set<T> merging = new HashSet<>();

    private final ConcurrentSkipListSet<T> segments = new ConcurrentSkipListSet<>();

    View() {
        this(Collections.emptyList());
    }

    View(Collection<T> initial) {
        refCounter.incrementAndGet();
        segments.addAll(initial);
    }

    @Override
    public Iterator<T> iterator() {
        return segments.iterator();
    }

    public Iterator<T> descendingIterator() {
        return segments.descendingIterator();
    }

    public T head() {
        return segments.first();
    }

    public T tail() {
        return segments.last();
    }

    boolean isEmpty() {
        return segments.isEmpty();
    }

    @Override
    public void close() {
        boolean markedForDeletion = !this.markedForDeletion.isEmpty();
        int refs = refCounter.decrementAndGet();
        if (refs == 0 && markedForDeletion) {
            for (T item : this.markedForDeletion) {
                item.delete();
            }
        }
    }

    synchronized View<T> append(T item) {
        View<T> copy = new View<>(segments);
        copy.segments.add(item);
        copy.merging.addAll(merging);
        return copy;
    }

    synchronized MergeHandle<T> createMerge(int level, int minItems, int maxItems, File result) {
        List<T> items = new ArrayList<>();
        for (T segment : segments) {
            if (items.size() >= maxItems) {
                break;
            }
            if (DirectoryUtils.level(segment) == level) {
                items.add(segment);
            }
        }
        if (items.size() < minItems) {
            return null;
        }
        merging.addAll(items);
        return new MergeHandle<>(result, items, this.acquire());
    }

    synchronized View<T> merge(MergeHandle<T> mergeItem) {
        if (!segments.containsAll(mergeItem.sources)) {
            throw new IllegalStateException("Merge segment mismatch");
        }

        for (T segment : segments) {
            markForDeletion(segment);
        }

        View<T> copy = new View<>();
        copy.segments.addAll(segments);
        copy.segments.removeAll(mergeItem.sources);
        copy.segments.add(mergeItem.replacement);
        return copy;
    }

    private void markForDeletion(T item) {
        assert refCounter.get() > 0 : "No valid reference to this view";
        markedForDeletion.add(item);
    }

    View<T> acquire() {
        refCounter.incrementAndGet();
        return this;
    }
}

package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;
import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.Iterators;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import static io.joshworks.es2.directory.DirectoryUtils.initDirectory;
import static io.joshworks.es2.directory.DirectoryUtils.level;
import static io.joshworks.es2.directory.DirectoryUtils.segmentFileName;
import static io.joshworks.es2.directory.DirectoryUtils.segmentIdx;

/**
 * Not Thread safe, synchronization must be done outside
 * Mainly because EventStore needs to sync on both Log and Index a the same time, adding here would just be unecessary overhead
 * head() does not need sync as it does not change
 */
public class SegmentDirectory<T extends SegmentFile> implements Iterable<T>, Closeable {

    private final TreeSet<T> segments = new TreeSet<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Set<T> merging = new HashSet<>();

    private final File root;
    private final String extension;

    public SegmentDirectory(File root, String extension) {
        this.root = root;
        this.extension = extension;
        initDirectory(root);
    }

    public File newHead() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            long segIdx = 0;
            if (!segments.isEmpty()) {
                T currentHead = segments.first();
                int headLevel = level(currentHead);
                if (headLevel == 0) {
                    segIdx = segmentIdx(currentHead) + 1;
                }
            }
            String name = segmentFileName(segIdx, 0, extension);
            return new File(root, name);
        } finally {
            lock.unlock();
        }
    }

    //modifications must be synchronized
    public void append(T newSegmentHead) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (segments.first().compareTo(newSegmentHead) <= 0) {
                throw new IllegalStateException("New segment won't be new head");
            }
            segments.add(newSegmentHead);
            assert segments.first().equals(newSegmentHead);
        } finally {
            lock.unlock();
        }
    }

    public void loadSegments(Function<File, T> fn) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            Files.list(root.toPath())
                    .map(Path::toFile)
                    .filter(this::matchExtension)
                    .map(fn)
                    .forEach(segments::add);

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to load segments", e);
        } finally {
            lock.unlock();
        }

    }

    public MergeHandle<T> createMerge(int level, int minItems, int maxItems, File result) {
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
        return new MergeHandle<>(result, items, rwLock, merging, segments);
    }


    private boolean matchExtension(File file) {
        return file.getName().endsWith(extension);
    }

    @Override
    public void close() {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            for (T segment : segments) {
                segment.close();
            }
            segments.clear();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterators.CloseableIterator<T> iterator() {
        Lock lock = rwLock.readLock();
        lock.lock();
        return new SegmentIterator(lock, segments.iterator());
    }

    public Iterators.CloseableIterator<T> reverse() {
        Lock lock = rwLock.readLock();
        lock.lock();
        return new SegmentIterator(lock, segments.descendingIterator());
    }


    private class SegmentIterator implements Iterators.CloseableIterator<T> {

        private final Lock lock;
        private final Iterator<T> delegate;

        private SegmentIterator(Lock lock, Iterator<T> delegate) {
            this.lock = lock;
            this.delegate = delegate;
        }

        @Override
        public void close() {
            lock.unlock();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            return delegate.next();
        }
    }

}

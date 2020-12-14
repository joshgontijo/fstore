package io.joshworks.es2.directory;

import io.joshworks.es2.SegmentFile;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static io.joshworks.es2.directory.DirectoryUtils.level;
import static io.joshworks.es2.directory.DirectoryUtils.segmentIdx;

public class MergeHandle<T extends SegmentFile> implements Closeable {
    private final File replacement;
    private final List<T> sources;

    private final ReadWriteLock rwLock;
    private final Set<T> mergeList;
    private final Set<T> segments;

    private final AtomicBoolean merged = new AtomicBoolean();


    MergeHandle(File replacement, List<T> sources, ReadWriteLock rwLock, Set<T> mergeList, Set<T> segments) {
        this.replacement = replacement;
        this.sources = sources;
        this.rwLock = rwLock;
        this.mergeList = mergeList;
        this.segments = segments;
    }

    public File replacement() {
        return replacement;
    }

    //sync is required because 'replacement' is delete on close
    public synchronized void merge(T replacementSegment) {
        Lock lock = rwLock.writeLock();
        lock.lock();
        try {
            if (merged.get()) {
                return;
            }

            int expectedNextLevel = sources.stream().mapToInt(DirectoryUtils::level).max().getAsInt() + 1;
            long expectedSegmentIdx = sources.stream().mapToLong(DirectoryUtils::segmentIdx).min().getAsLong();

            int replacementLevel = level(replacementSegment);
            long replacementIdx = segmentIdx(replacementSegment);
            if (replacementLevel != expectedNextLevel) {
                throw new IllegalArgumentException("Expected level " + expectedNextLevel + ", got " + replacementLevel);
            }
            if (replacementIdx != expectedSegmentIdx) {
                throw new IllegalArgumentException("Expected segmentIdx " + expectedSegmentIdx + ", got " + replacementIdx);
            }

            for (T source : sources) {
                source.delete();
            }
            segments.add(replacementSegment);
            mergeList.removeAll(sources);
            merged.set(true);

        } finally {
            lock.unlock();
        }
    }

    @Override
    public synchronized void close() {
        if (merged.get()) {
            return;
        }
        mergeList.removeAll(sources);
        replacement.delete();

    }
}

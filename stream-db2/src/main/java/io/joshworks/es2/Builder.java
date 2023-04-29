package io.joshworks.es2;

import io.joshworks.es2.sstable.CompactionConfig;
import io.joshworks.fstore.core.util.Size;

import java.nio.file.Path;

public class Builder {
    final MemTableConfig memTable = new MemTableConfig();
    final WriterConfig writer = new WriterConfig();
    final CompactionConfig compaction = new CompactionConfig();
    final TransactionLogConfig log = new TransactionLogConfig();

    Builder() {
    }

    public MemTableConfig memtable() {
        return memTable;
    }

    public TransactionLogConfig log() {
        return log;
    }

    public WriterConfig writer() {
        return writer;
    }

    public CompactionConfig compaction() {
        return compaction;
    }


    public EventStore open(Path path) {
        return new EventStore(path, this);
    }


    public static class WriterConfig {
        long poolTimeMs = 2;
        long waitTimeout = 50;
        int maxEntries = 1000;

        public WriterConfig poolTimeMs(long poolTimeMs) {
            this.poolTimeMs = poolTimeMs;
            return this;
        }

        public WriterConfig waitTimeout(long waitTimeout) {
            this.waitTimeout = waitTimeout;
            return this;
        }

        public WriterConfig maxEntries(int maxEntries) {
            this.maxEntries = maxEntries;
            return this;
        }
    }

    public static class TransactionLogConfig {
        long size = Size.MB.of(100);
        FlushMode flushMode = FlushMode.ON_WRITE;

        public TransactionLogConfig size(long size) {
            this.size = size;
            return this;
        }

        public TransactionLogConfig flushMode(FlushMode flushMode) {
            this.flushMode = flushMode;
            return this;
        }
    }

    public enum FlushMode {
        OS,
        ON_WRITE
    }

    public static class MemTableConfig {
        int size = Size.MB.ofInt(50);
        boolean direct = false;

        public MemTableConfig size(int sizeInBytes) {
            this.size = sizeInBytes;
            return this;
        }

        public MemTableConfig direct(boolean direct) {
            this.direct = direct;
            return this;
        }
    }


}

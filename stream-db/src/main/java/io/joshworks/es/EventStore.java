package io.joshworks.es;

import io.joshworks.es.events.SystemStreams;
import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.es.reader.StoreReader;
import io.joshworks.es.writer.StoreWriter;
import io.joshworks.es.writer.WriteEvent;
import io.joshworks.es.writer.WriteTask;
import io.joshworks.fstore.core.util.Memory;

import java.io.File;
import java.nio.ByteBuffer;

public class EventStore {

    private final Log log;
    private final Index index;
    private final StoreWriter writer;
    private final StoreReader reader;

    private static final int READ_MAX_ITEMS = 50;
    private static final int WRITE_MAX_ITEMS = 100;
    private static final int WRITE_BUFFER_SIZE = Memory.PAGE_SIZE;
    private static final int WRITE_POOL_WAIT = 50;

    public EventStore(File root, int logSize, int indexEntries, int blockSize, int versionCacheSize) {
        this.log = new Log(root, logSize);
        this.index = new Index(root, indexEntries, blockSize, versionCacheSize);
        this.writer = new StoreWriter(log, index, WRITE_MAX_ITEMS, WRITE_BUFFER_SIZE, WRITE_POOL_WAIT);
        this.reader = new StoreReader(log, index);
        this.writer.start();
    }

    public int version(long stream) {
        return index.version(stream);
    }

    public synchronized void linkTo(String srcStream, int srcVersion, String dstStream, int expectedVersion) {
        writer.submit(writer -> {

            long srcStreamHash = StreamHasher.hash(srcStream);
            long dstStreamHash = StreamHasher.hash(dstStream);

            IndexEntry ie = writer.findEquals(new IndexKey(srcStreamHash, srcVersion));
            if (ie == null) {
                throw new IllegalArgumentException("No such event " + IndexKey.toString(srcStream, srcVersion));
            }
            int dstVersion = writer.nextVersion(dstStreamHash, expectedVersion);

            WriteEvent linkTo = SystemStreams.linkTo(srcStream, srcVersion, dstStream, dstVersion);

            long logAddress = writer.appendToLog(linkTo);

            writer.adToIndex(new IndexEntry(dstStreamHash, dstVersion, logAddress));
        });
    }

    public WriteTask append(WriteEvent event) {
        return writer.submit(writer -> {
            long stream = StreamHasher.hash(event.stream);
            int version = writer.nextVersion(stream, event.expectedVersion);
            event.version = version;

            long logAddress = writer.appendToLog(event);
            writer.adToIndex(new IndexEntry(stream, version, logAddress));
        });
    }

    public int get(long stream, int version, ByteBuffer dst) {
        return reader.get(stream, version, READ_MAX_ITEMS, dst);
    }

    public int get(String stream, int version, ByteBuffer dst) {
        return get(StreamHasher.hash(stream), version, dst);
    }

    public void flush() {
        writer.commit();
        index.flush();
    }
}

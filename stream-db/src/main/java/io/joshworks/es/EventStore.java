package io.joshworks.es;

import io.joshworks.es.events.LinkToEvent;
import io.joshworks.es.events.SystemStreams;
import io.joshworks.es.events.WriteEvent;
import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.es.reader.StoreReader;
import io.joshworks.es.writer.StoreWriter;
import io.joshworks.es.writer.WriteTask;
import io.joshworks.fstore.core.util.Memory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;

public class EventStore implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EventStore.class);

    private final Log log;
    private final Index index;
    private final StoreWriter writer;
    private final StoreReader reader;
    private final StoreLock storeLock = new StoreLock();

    private static final int READ_MAX_ITEMS = 50;
    private static final int WRITE_BATCH_MAX_ITEMS = 100;
    private static final int WRITE_QUEUE_SIZE = 200;
    private static final int WRITE_BUFFER_SIZE = Memory.PAGE_SIZE;
    private static final int READ_BUFFER_SIZE = Memory.PAGE_SIZE;
    private static final int WRITE_POOL_WAIT = 50;

    public EventStore(File root, int logSize, int indexEntries, int blockSize) {
        this.log = new Log(root, logSize);
        this.index = new Index(root, indexEntries, blockSize);
        this.writer = new StoreWriter(log, index, WRITE_QUEUE_SIZE, WRITE_BATCH_MAX_ITEMS, WRITE_BUFFER_SIZE, WRITE_POOL_WAIT);
        this.reader = new StoreReader(log, index, storeLock, READ_MAX_ITEMS, READ_BUFFER_SIZE);

        this.restore();

        this.writer.start();
    }

    private void restore() {
        long stream = StreamHasher.hash(SystemStreams.INDEX_STREAM);
        int version = index.version(stream);
        IndexEntry lastFlush = index.get(new IndexKey(stream, version));
        long logAddress = lastFlush == null ? 0 : lastFlush.logAddress();

        logger.info("Restoring index from {}", logAddress);

        long items = log.restore(logAddress, (pos, bb) -> {
            if (!Event.isValid(bb)) {
                throw new IllegalStateException("Invalid event found at " + pos);
            }
            if (SystemStreams.isIndexFlush(bb)) {
                if (pos == logAddress) {//not the start event, something is wrong
                    return; //ignore this event
                }
                throw new IllegalStateException("Unexpected Index flush event");
            }
            index.append(new IndexEntry(Event.stream(bb), Event.version(bb), pos));
        });

        logger.info("Restored {} entries", items);

    }

    public int version(String stream) {
        return version(StreamHasher.hash(stream));
    }

    public int version(long stream) {
        return index.version(stream);
    }

    public void linkTo(LinkToEvent linkTo) {
        writer.enqueue(linkTo);
    }

    public WriteTask append(WriteEvent event) {
        return writer.enqueue(event);
    }

    public int get(long stream, int version, ByteBuffer dst) {
        return reader.get(stream, version, dst);
    }

    public int get(String stream, int version, ByteBuffer dst) {
        return get(StreamHasher.hash(stream), version, dst);
    }

    public void flushWrites() {
        writer.commit();
    }

    public void flushIndex() {
        flushWrites();
        index.flush();
    }

    @Override
    public void close() {
        writer.shutdown();
        reader.close();
        index.close();
        log.close();

    }
}

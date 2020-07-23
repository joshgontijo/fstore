package io.joshworks.es;

import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;
import io.joshworks.es.writer.WriteEvent;
import io.joshworks.es.writer.WriterThread;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EventStore {

    private final Log log;
    private final Index index;
    private final WriterThread writerThread;

    public EventStore(File root, int logSize, int indexEntries, int blockSize, int versionCacheSize) {
        this.log = new Log(root, logSize);
        this.index = new Index(root, indexEntries, blockSize, versionCacheSize);
        this.writerThread = new WriterThread(log, index, 100, 4096, 100);
        this.writerThread.start();
    }

    public int version(long stream) {
        return index.version(stream);
    }

    public synchronized void linkTo(String srcStream, int srcVersion, String dstStream, int expectedVersion) {
        writerThread.submit(writer -> {

            long srcStreamHash = StreamHasher.hash(srcStream);
            long dstStreamHash = StreamHasher.hash(dstStream);

            IndexEntry ie = writer.findEquals(new IndexKey(srcStreamHash, srcVersion));
            if (ie == null) {
                throw new IllegalArgumentException("No such event " + IndexKey.toString(srcStream, srcVersion));
            }
            int dstVersion = writer.nextVersion(dstStreamHash, expectedVersion);

            WriteEvent linkToEvent = createLinkToEvent(srcVersion, dstStream, expectedVersion, srcStreamHash);

            long logAddress = writer.appendToLog(linkToEvent);
            int eventSize = Event.sizeOf(linkToEvent);

            writer.adToIndex(new IndexEntry(dstStreamHash, dstVersion, eventSize, logAddress));
        });
    }

    private WriteEvent createLinkToEvent(int srcVersion, String dstStream, int expectedVersion, long srcStreamHash) {
        WriteEvent linktoEv = new WriteEvent();
        linktoEv.stream = dstStream;
        linktoEv.type = "LINK_TO";
        linktoEv.timestamp = System.currentTimeMillis();
        linktoEv.expectedVersion = expectedVersion;
        linktoEv.metadata = new byte[0];
        linktoEv.data = IndexKey.toString(dstStream, srcVersion).getBytes(StandardCharsets.UTF_8);
        //TODO add linkTo attribute ?
        return linktoEv;
    }

    public void append(WriteEvent event) {
        var result = writerThread.submit(writer -> {
            long stream = StreamHasher.hash(event.stream);
            int version = writer.nextVersion(stream, event.expectedVersion);
            event.version = version;

            //this is critical, event size must be always the same otherwise logPos will be wrong
            int eventSize = Event.sizeOf(event);
            long logAddress = writer.appendToLog(event);
            writer.adToIndex(new IndexEntry(stream, version, eventSize, logAddress));
        });

    }

    public int get(IndexKey key, int count, ByteBuffer dst) {

        QueryPlanner planner = new QueryPlanner();
        boolean success = planner.prepare(index, key, count, dst.remaining());
        if (!success) {
            return 0;
        }

        return planner.execute(log, dst);
    }

}

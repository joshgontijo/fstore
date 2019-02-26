package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;

public class CompletedHeader implements Header {

    private static final Serializer<CompletedHeader> serializer = new CompletedHeaderSerializer();

    public final int level;
    public final long entries;
    public final long logicalSize; //LOG_DATA size (data - headers - EOF)
    public final long timestamp;

    CompletedHeader(long entries, int level, long timestamp, long logicalSize) {
        this.entries = entries;
        this.level = level;
        this.timestamp = timestamp;
        this.logicalSize = logicalSize;
    }

    public static CompletedHeader write(Storage storage, long entries, int level, long timestamp, long logicalSize) {
        CompletedHeader header = new CompletedHeader(entries, level, timestamp, logicalSize);
        HeaderUtil.write(storage, header, serializer);
        return header;
    }

    public static CompletedHeader read(Storage storage) {
        long fileLength = storage.length();
        long headerPos = fileLength - (Header.BYTES * 2);
        return HeaderUtil.read(storage, headerPos, serializer);
    }

}

package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;

public class DeletedHeader implements Header {

    private static final Serializer<DeletedHeader> serializer = new DeletedHeaderSerializer();

    public final long timestamp;

    DeletedHeader(long timestamp) {
        this.timestamp = timestamp;
    }

    public static DeletedHeader write(Storage storage, long timestamp) {
        DeletedHeader header = new DeletedHeader(timestamp);
        HeaderUtil.write(storage, header, serializer);
        return header;
    }

    public static DeletedHeader read(Storage storage) {
        long fileLength = storage.length();
        long headerPos = fileLength - (Header.BYTES);
        return HeaderUtil.read(storage, headerPos, serializer);
    }

}

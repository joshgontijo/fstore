package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;

public class CreatedHeader implements Header {

    private static final Serializer<CreatedHeader> serializer = new CreatedHeaderSerializer();

    public final String magic;
    public final long created;
    public final Type type;
    public final long fileSize;

    CreatedHeader(String magic, long created, Type type, long fileSize) {
        this.magic = magic;
        this.created = created;
        this.type = type;
        this.fileSize = fileSize;
    }

    public static CreatedHeader write(Storage storage, String magic, long created, Type type, long fileSize) {
        CreatedHeader header = new CreatedHeader(magic, created, type, fileSize);
        HeaderUtil.write(storage, header, serializer);
        return header;
    }

    public static CreatedHeader read(Storage storage) {
        return HeaderUtil.read(storage, 0, serializer);
    }


}

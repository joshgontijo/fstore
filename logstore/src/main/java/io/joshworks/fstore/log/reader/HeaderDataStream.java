package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.DataStream;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;
import io.joshworks.fstore.log.segment.Header;

import java.nio.ByteBuffer;
import java.util.Random;

public abstract class HeaderDataStream<T> extends DataStream<T> {

    //from Log


    public HeaderDataStream() {
        this(DEFAULT_CHECKUM_PROB);
    }



//    static long write(Storage storage, ByteBuffer bytes) {
//        int entrySize = bytes.remaining();
//        ByteBuffer bb = ByteBuffer.allocate(HEADER_OVERHEAD + entrySize);
//        bb.putInt(entrySize);
//        bb.putInt(Checksum.crc32(bytes));
//        bb.put(bytes);
//        bb.putInt(entrySize);
//
//        bb.flip();
//        return storage.write(bb);
//    }


    public HeaderDataStream(double checksumProb) {

    }



}

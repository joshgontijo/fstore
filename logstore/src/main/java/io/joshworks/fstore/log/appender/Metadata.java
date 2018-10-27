package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.log.utils.LogFileUtils;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Metadata {

    static final int MAGIC_SIZE = 36 + Integer.BYTES; //VSTRING
    static final int METADATA_SIZE = (Integer.BYTES * 2) + Long.BYTES + (Byte.BYTES * 2) + MAGIC_SIZE;

    final String magic;
    final long segmentSize;
    final int compactionThreshold;
    final boolean flushAfterWrite;
    final boolean asyncFlush;

    private Metadata(String magic, long segmentSize, int compactionThreshold, boolean flushAfterWrite, boolean asyncFlush) {
        this.magic = magic;
        this.segmentSize = segmentSize;
        this.flushAfterWrite = flushAfterWrite;
        this.asyncFlush = asyncFlush;
        this.compactionThreshold = compactionThreshold;
    }

    public static Metadata readFrom(File directory) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = StorageProvider.of(StorageMode.RAF).open(file)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);
            storage.read(0, bb);
            bb.flip();


            String magic = Serializers.VSTRING.fromBytes(bb);
            long logSize = bb.getLong();
            int maxSegmentsPerLevel = bb.getInt();
            boolean flushAfterWrite = bb.get() == 1;
            boolean asyncFlush = bb.get() == 1;

            return new Metadata(magic, logSize, maxSegmentsPerLevel, flushAfterWrite, asyncFlush);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static Metadata write(File directory, long segmentSize, int maxSegmentsPerLevel, boolean flushAfterWrite, boolean asyncFlush) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = StorageProvider.of(StorageMode.RAF).create(file, METADATA_SIZE)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);

            String magic = createMagic();

            bb.put(Serializers.VSTRING.toBytes(magic));
            bb.putLong(segmentSize);
            bb.putInt(maxSegmentsPerLevel);
            bb.put(flushAfterWrite ? (byte) 1 : 0);
            bb.put(asyncFlush ? (byte) 1 : 0);

            bb.flip();
            storage.write(bb);
            return new Metadata(magic, segmentSize, maxSegmentsPerLevel, flushAfterWrite, asyncFlush);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private static String createMagic() {
        return UUID.randomUUID().toString();
    }

}

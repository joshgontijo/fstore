package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Metadata {

    static final int MAGIC_SIZE = 36 + Integer.BYTES; //VSTRING
    static final int METADATA_SIZE = (Integer.BYTES * 2) + Long.BYTES + (Byte.BYTES * 3) + MAGIC_SIZE;

    final String magic;
    final long logSize;
    final int footerSize;
    final int maxSegmentsPerLevel;
    final boolean mmap;
    final boolean flushAfterWrite;
    final boolean asyncFlush;

    private Metadata(String magic, long logSize, int footerSize, int maxSegmentsPerLevel, boolean mmap, boolean flushAfterWrite, boolean asyncFlush) {
        this.magic = magic;
        this.logSize = logSize;
        this.footerSize = footerSize;
        this.mmap = mmap;
        this.flushAfterWrite = flushAfterWrite;
        this.asyncFlush = asyncFlush;
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
    }

    public static Metadata readFrom(File directory) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = StorageProvider.raf().open(file)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);
            storage.read(0, bb);
            bb.flip();


            String magic = Serializers.VSTRING.fromBytes(bb);
            long logSize = bb.getLong();
            int footerSize = bb.getInt();
            int maxSegmentsPerLevel = bb.getInt();
            boolean mmap = bb.get() == 1;
            boolean flushAfterWrite = bb.get() == 1;
            boolean asyncFlush = bb.get() == 1;

            return new Metadata(magic, logSize, footerSize, maxSegmentsPerLevel, mmap, flushAfterWrite, asyncFlush);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static Metadata create(File directory, long logSize, int footerSize, int maxSegmentsPerLevel, boolean mmap, boolean flushAfterWrite, boolean asyncFlush) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = StorageProvider.raf().create(file, METADATA_SIZE)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);

            String magic = createMagic();

            bb.put(Serializers.VSTRING.toBytes(magic));
            bb.putLong(logSize);
            bb.putInt(footerSize);
            bb.putInt(maxSegmentsPerLevel);
            bb.put(mmap ? (byte) 1 : 0);
            bb.put(flushAfterWrite ? (byte) 1 : 0);
            bb.put(asyncFlush ? (byte) 1 : 0);

            bb.flip();
            storage.write(bb);
            return new Metadata(magic, logSize, footerSize, maxSegmentsPerLevel, mmap, flushAfterWrite, asyncFlush);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }

    }

    private static String createMagic() {
        return UUID.randomUUID().toString();
    }

}

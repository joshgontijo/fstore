package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.utils.LogFileUtils;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class Metadata {

    static final int MAGIC_SIZE = 8 + Integer.BYTES; //VSTRING
    static final int METADATA_SIZE = (Integer.BYTES * 3) + Long.BYTES + MAGIC_SIZE;

    final String magic;
    final long segmentSize;
    final int compactionThreshold;
    final FlushMode flushMode;

    private Metadata(String magic, long segmentSize, int compactionThreshold, FlushMode flushMode) {
        this.magic = magic;
        this.segmentSize = segmentSize;
        this.flushMode = flushMode;
        this.compactionThreshold = compactionThreshold;
    }

    public static Metadata readFrom(File directory) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = Storage.open(file, StorageMode.RAF)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);
            storage.read(0, bb);
            bb.flip();


            String magic = Serializers.VSTRING.fromBytes(bb);
            long logSize = bb.getLong();
            int maxSegmentsPerLevel = bb.getInt();
            int flushMode = bb.getInt();

            return new Metadata(magic, logSize, maxSegmentsPerLevel, FlushMode.of(flushMode));
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static Metadata write(File directory, long segmentSize, int maxSegmentsPerLevel, FlushMode flushMode) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = Storage.create(file, StorageMode.RAF, METADATA_SIZE)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);

            String magic = createMagic();

            bb.put(Serializers.VSTRING.toBytes(magic));
            bb.putLong(segmentSize);
            bb.putInt(maxSegmentsPerLevel);
            bb.putInt(flushMode.code);

            bb.flip();
            storage.write(bb);
            return new Metadata(magic, segmentSize, maxSegmentsPerLevel, flushMode);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    private static String createMagic() {
        return UUID.randomUUID().toString().substring(0, 8);
    }

}

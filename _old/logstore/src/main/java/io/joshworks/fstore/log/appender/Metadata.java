package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.utils.LogFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Metadata {

    static final int METADATA_SIZE = (Integer.BYTES * 3) + Long.BYTES;

    final long segmentSize;
    final int compactionThreshold;
    final FlushMode flushMode;

    private Metadata(long segmentSize, int compactionThreshold, FlushMode flushMode) {
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

            long logSize = bb.getLong();
            int maxSegmentsPerLevel = bb.getInt();
            int flushMode = bb.getInt();

            return new Metadata(logSize, maxSegmentsPerLevel, FlushMode.of(flushMode));
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static Metadata write(File directory, long segmentSize, int maxSegmentsPerLevel, FlushMode flushMode) {
        File file = new File(directory, LogFileUtils.METADATA_FILE);
        try (Storage storage = Storage.create(file, StorageMode.RAF, METADATA_SIZE)) {
            ByteBuffer bb = ByteBuffer.allocate(METADATA_SIZE);

            bb.putLong(segmentSize);
            bb.putInt(maxSegmentsPerLevel);
            bb.putInt(flushMode.code);

            bb.flip();
            storage.write(bb);
            return new Metadata(segmentSize, maxSegmentsPerLevel, flushMode);
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

}

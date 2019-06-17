package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.utils.LogFileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class State implements Closeable {

    private static final int BYTES = Long.BYTES * 3;

    private final Storage storage;

    private long position;
    private long entryCount;
    private long lastRollTime;

    private boolean dirty;

    private State(Storage storage, long position, long entryCount, long lastRollTime) {
        this.storage = storage;
        this.position = position;
        this.entryCount = entryCount;
        this.lastRollTime = lastRollTime;
    }

    public void position(long position) {
        this.position = position;
        this.dirty = true;
    }

    public void addEntryCount(long delta) {
        this.entryCount += delta;
        this.dirty = true;
    }

    public void incrementEntryCount() {
        this.entryCount++;
        this.dirty = true;
    }

    public void lastRollTime(long lastRollTime) {
        this.lastRollTime = lastRollTime;
        this.dirty = true;
    }

    public long position() {
        return position;
    }

    public long entryCount() {
        return entryCount;
    }

    public long lastRollTime() {
        return lastRollTime;
    }

    public static State readFrom(File directory) {
        File file = new File(directory, LogFileUtils.STATE_FILE);
        Storage storage = null;
        try {
            storage = Storage.open(file, StorageMode.RAF);
            ByteBuffer data = ByteBuffer.allocate(BYTES);
            int read = storage.read(0, data);
            if (read != BYTES) {
                throw new IllegalStateException("Expected " + BYTES + " bytes data, got " + read);
            }

            data.flip();

            long lastPosition = data.getLong();
            long entryCount = data.getLong();
            long lastRollTime = data.getLong();

            return new State(storage, lastPosition, entryCount, lastRollTime);

        } catch (Exception e) {
            IOUtils.closeQuietly(storage);
            throw e;
        }
    }

    public static State empty(File directory) {
        File file = new File(directory, LogFileUtils.STATE_FILE);
        Storage storage = Storage.create(file, StorageMode.RAF, BYTES);
        return new State(storage, Log.START, 0L, System.currentTimeMillis());
    }

    public synchronized void flush() {
        if (!dirty) {
            return;
        }

        ByteBuffer bb = ByteBuffer.allocate(BYTES);
        bb.putLong(position);
        bb.putLong(entryCount);
        bb.putLong(lastRollTime);
        bb.flip();

        write(bb);
    }

    private void write(ByteBuffer data) {
        try {
            storage.writePosition(0);
            int written = storage.write(data);
            if (written == Storage.EOF) {
                throw new IllegalStateException("Failed to write state got EOF from storage");
            }
            storage.flush();
            dirty = false;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public String toString() {
        return "{position=" + position +
                ", entryCount=" + entryCount +
                ", lastRollTime=" + lastRollTime +
                '}';
    }

    @Override
    public void close() {
        try {
            storage.close();
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }
}

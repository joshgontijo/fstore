package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.RuntimeIOException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class RafStorage extends DiskStorage {

    public RafStorage(File target, long length, RandomAccessFile raf) {
        super(target, length, raf);
    }

    /**
     * Using channel.write(buffer, position) will result in a pwrite() sys call
     */
    @Override
    public int write(ByteBuffer data) {
        Storage.ensureNonEmpty(data);
        try {
            int written = 0;
            while (data.hasRemaining()) {
                written += channel.write(data);
            }
            long updatedPos = position.addAndGet(written);
            if (updatedPos > length()) {
                length.set(updatedPos);
            }
            return written;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    @Override
    public int read(long position, ByteBuffer dst) {
        try {
            if (!hasAvailableData(position)) {
                return EOF;
            }
            long writePosition = writePosition();
            int read = 0;
            int totalRead = 0;
            while (dst.hasRemaining() && read >= 0) {
                long currReadPosition = position + totalRead;
                int remaining = dst.remaining();
                long available = Math.min(remaining, writePosition - currReadPosition);
                int limit = (int) Math.min(remaining, available);
                dst.limit(limit);

                read = channel.read(dst, currReadPosition);
                if (read > 0) {
                    totalRead += read;
                }
            }
            return totalRead;
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

}

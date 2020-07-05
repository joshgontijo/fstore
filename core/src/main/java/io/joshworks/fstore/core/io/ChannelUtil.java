package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class ChannelUtil {

    private ChannelUtil() {

    }

    /**
     * Transfer all available data from source channel at the point this method is called to the destination channel
     */
    public static long transferFully(FileChannel src, WritableByteChannel dst, long startPos) {
        try {
            long totalSize = src.size();
            long transferred = 0;
            while (transferred < totalSize) {
                long count = src.transferTo(startPos + transferred, totalSize, dst);
                if (count < 0) {
                    throw new IOException("Channel closed");
                }
                transferred += count;
            }
            return transferred;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to transfer data", e);
        }
    }

    /**
     * Transfer all available data from source channel at the point this method is called to the destination channel
     */
    public static long transferFully(FileChannel src, WritableByteChannel dst) {
        return transferFully(src, dst, 0);
    }

}

package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

public class Channels {

    private Channels() {

    }

    /**
     * Read from this channel until the destination buffer is full, caller must ensure the channel will provide enough data otherwise this will loop forever
     */
    public static int readFully(FileChannel src, long offset, ByteBuffer dst) {
        try {
            int totalRead = 0;
            do {
                int read = src.read(dst, offset + totalRead);
                checkClosed(read);
                totalRead += read;
            } while (dst.hasRemaining());
            return totalRead;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read data", e);
        }
    }

    /**
     * Read from this channel until the destination buffer is full OR end of file is reached.
     */
    public static int read(FileChannel src, long offset, ByteBuffer dst) {
        try {
            int totalRead = 0;
            do {
                int read = src.read(dst, offset + totalRead);
                checkClosed(read);
                totalRead += read;
            } while (dst.hasRemaining() && src.size() > offset + totalRead);
            return totalRead;
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read data", e);
        }
    }

    public static void transferFully(FileChannel src, SocketChannel dst) {
        try {
            long size = src.size();
            long transferred = 0;
            do {
                long written = src.transferTo(transferred, size - transferred, dst);
                checkClosed(written);
                if (written == 0 && transferred < size) {
                    await(dst, SelectionKey.OP_WRITE);
                }

                transferred += written;
            } while (transferred < size);


        } catch (Exception e) {
            throw new RuntimeIOException("Failed to transfer data", e);
        }
    }

    //TODO opening selector every time is not right
    public static void await(SelectableChannel channel, int op) throws IOException {
        try (Selector selector = Selector.open()) {
            final SelectionKey selectionKey;
            try {
                selectionKey = channel.register(selector, op);
            } catch (ClosedChannelException e) {
                return;
            }
            selector.select();
            selector.selectedKeys().clear();
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }
            selectionKey.cancel();
            selector.selectNow();
        }
    }

    public static long transferFully(FileChannel src, WritableByteChannel dst) {
        return transferFully(src, 0, dst);
    }

    public static long transferFully(FileChannel src, long startPos, WritableByteChannel dst) {
        try {
            long size = src.size() - startPos;
            long transferred = 0;
            do {
                long written = src.transferTo(startPos + transferred, size - transferred, dst);
                checkClosed(written);
                transferred += written;
            } while (transferred < size);

            return transferred;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to transfer data", e);
        }
    }

    public static int writeFully(WritableByteChannel dst, ByteBuffer buffer) {
        try {
            int total = 0;
            while (buffer.hasRemaining()) {
                int written = dst.write(buffer);
                checkClosed(written);
                total += written;
            }
            return total;

        } catch (Exception e) {
            throw new RuntimeIOException(e);
        }
    }

    public static long writeFully(GatheringByteChannel dst, ByteBuffer[] buffers, int offset, int count) {
        try {
            long remaining = Buffers.remaining(buffers, offset, count);
            if (remaining == 0) {
                return 0;
            }

            long written = 0;
            while (written < remaining) {
                long w = dst.write(buffers, offset, count);
                checkClosed(w);
                written += w;
            }
            return written;

        } catch (Exception e) {
            throw new RuntimeIOException(e);
        }
    }

    private static void checkClosed(long written) {
        if (written == Storage.EOF) {
            throw new RuntimeIOException("Channel closed");
        }
    }
}

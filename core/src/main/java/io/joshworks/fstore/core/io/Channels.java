package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

public class Channels {

    private Channels() {

    }

    private static void transferFully(FileChannel src, SocketChannel dst) {
        try {
            long size = src.size();
            long transferred = 0;
            do {
                long written = src.transferTo(transferred, size - transferred, dst);
                if (written == -1) {
                    throw new RuntimeIOException("Channel closed");
                }
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
        try {
            long size = src.size();
            long transferred = 0;
            do {
                long written = src.transferTo(transferred, size - transferred, dst);
                if (written == -1) {
                    throw new RuntimeIOException("Channel closed");
                }
                transferred += written;
            } while (transferred < size);

            return transferred;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to transfer data", e);
        }
    }

}

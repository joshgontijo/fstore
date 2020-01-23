package nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;

public interface ChannelWriter {

    default void doWrite(final ByteBuffer buffer, final SocketChannel channel) throws IOException {
        if (Objects.isNull(buffer) || Objects.isNull(channel)) {
            throw new IllegalArgumentException("Required buffer and channel.");
        }

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
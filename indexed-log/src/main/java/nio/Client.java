package nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.UUID;

public final class Client implements ChannelWriter {

    private final InetSocketAddress hostAddress;

    public static void main(final String[] args) {
        new Client(9999).start(UUID.randomUUID().toString());
    }

    private Client(final int port) {
        this.hostAddress = new InetSocketAddress(port);
    }

    private void start(final String message) {
        assert message != null && !message.isEmpty();

        try (SocketChannel client = SocketChannel.open(this.hostAddress)) {

            final ByteBuffer buffer = ByteBuffer.wrap((message + Constants.END_MESSAGE_MARKER).trim().getBytes());

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                doWrite(buffer, client);
                buffer.clear();
                if (i % 1000000 == 0) System.out.println("SENT: " + i);
            }


            final StringBuilder echo = new StringBuilder();
            doRead(echo, buffer, client);

            System.out.println(String.format("Message :\t %s \nEcho    :\t %s", message, echo.toString().replace(Constants.END_MESSAGE_MARKER, "")));
        } catch (IOException e) {
            throw new RuntimeException("Unable to communicate with server.", e);
        }
    }

    private void doRead(final StringBuilder data, final ByteBuffer buffer, final SocketChannel channel) throws IOException {
        assert !Objects.isNull(data) && !Objects.isNull(buffer) && !Objects.isNull(channel);

        while (channel.read(buffer) != -1) {
            data.append(new String(buffer.array()).trim());
            buffer.clear();
        }
    }
}
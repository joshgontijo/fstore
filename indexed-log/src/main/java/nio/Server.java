package nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class Server implements ChannelWriter, Runnable {

    private static final int BUFFER_SIZE = 1024;

    private final int port;
    private final Map<SocketChannel, StringBuilder> session;



    public static void main(final String[] args) {
        new Server(9999).run();
    }

    private Server(final int port) {
        this.port = port;
        this.session = new HashMap<>();
    }

    @Override
    public void run() {
        try (Selector selector = Selector.open(); ServerSocketChannel channel = ServerSocketChannel.open()) {
            initChannel(channel, selector);

            while (!Thread.currentThread().isInterrupted()) {
                if (selector.isOpen()) {
                    final int numKeys = selector.select(sk -> handleKeys(channel, sk));
                } else {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to start server.", e);
        } finally {
            this.session.clear();
        }
    }

    private void initChannel(final ServerSocketChannel channel, final Selector selector) throws IOException {
        assert !Objects.isNull(channel) && !Objects.isNull(selector);

        channel.socket().setReuseAddress(true);
        channel.configureBlocking(false);
        channel.socket().bind(new InetSocketAddress(this.port));
        channel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void handleKeys(final ServerSocketChannel channel, final SelectionKey key) {
        assert !Objects.isNull(key) && !Objects.isNull(channel);
        try {
            if (key.isValid()) {
                if (key.isAcceptable()) {
                    doAccept(channel, key);
                } else if (key.isReadable()) {
                    doRead(key);
                } else {
                    throw new UnsupportedOperationException("Key not supported by server.");
                }
            } else {
                throw new UnsupportedOperationException("Key not valid.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (mustEcho(key)) {
                try {
                    doEcho(key);
                    cleanUp(key);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void doAccept(final ServerSocketChannel channel, final SelectionKey key) throws IOException {
        assert !Objects.isNull(key) && !Objects.isNull(channel);

        final SocketChannel client = channel.accept();
        client.configureBlocking(false);
        client.register(key.selector(), SelectionKey.OP_READ);

        // Create a session for the incoming connection
        this.session.put(client, new StringBuilder());
    }

    private void doRead(final SelectionKey key) throws IOException {
        assert !Objects.isNull(key);

        final SocketChannel client = (SocketChannel) key.channel();
        final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

        final int bytesRead = client.read(buffer);
//        if (bytesRead > 0) {
//            this.session.get(client).append(new String(buffer.array()).trim());
//        } else if (bytesRead < 0) {
//            if (mustEcho(key)) {
//                doEcho(key);
//            }
//
//            cleanUp(key);
//        }
    }

    private void doEcho(final SelectionKey key) throws IOException {
        assert !Objects.isNull(key);

        StringBuilder stringBuilder = this.session.get(key.channel());
        final ByteBuffer buffer = ByteBuffer.wrap(stringBuilder.toString().trim().getBytes());
        doWrite(buffer, (SocketChannel) key.channel());
    }

    private boolean mustEcho(final SelectionKey key) {
        assert !Objects.isNull(key);
        return (key.channel() instanceof SocketChannel) && this.session.get(key.channel()).toString().contains(Constants.END_MESSAGE_MARKER);
    }

    private void cleanUp(final SelectionKey key) throws IOException {
        assert !Objects.isNull(key);

        this.session.remove((SocketChannel) key.channel());

        key.channel().close();
        key.cancel();
    }
}
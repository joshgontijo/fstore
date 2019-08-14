package io.joshworks.eventry.server.tcp_xnio;

import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.Channels;
import org.xnio.conduits.ConduitStreamSinkChannel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class EchoClient {

    public static void main(String[] args) throws Exception {
        final Charset charset = StandardCharsets.UTF_8;
        final Xnio xnio = Xnio.getInstance();
        final XnioWorker worker = xnio.createWorker(OptionMap.builder().set(Options.WORKER_IO_THREADS, 1).getMap());

        try {
            final IoFuture<StreamConnection> futureConnection = worker.openStreamConnection(new InetSocketAddress("localhost", 12346), new ChannelListener<StreamConnection>() {
                @Override
                public void handleEvent(StreamConnection channel) {
                    channel.getSinkChannel().setConduit(new KeepAliveConduit(channel, 2000));
                }
            }, OptionMap.builder().set(Options.WORKER_IO_THREADS, 1).getMap());
            final StreamConnection connection = futureConnection.get(); // throws exceptions
            ConduitStreamSinkChannel channel = connection.getSinkChannel();
            try {

                Thread.sleep(32000);

                for (int i = 0; i < 1000; i++) {
                    byte[] part1 = "Hello ".getBytes(StandardCharsets.UTF_8);
                    byte[] part2 = "World".getBytes(StandardCharsets.UTF_8);
                    ByteBuffer data = ByteBuffer.allocate(128);
                    data.putInt(part1.length + part2.length);
                    data.put(part1);
                    data.flip();
                    Channels.writeBlocking(channel, data);
                    Channels.flushBlocking(channel);

                    //part 2

                    data = ByteBuffer.allocate(128);
                    data.put(part2);
                    data.flip();
                    Channels.writeBlocking(channel, data);
                    Channels.flushBlocking(channel);



                    System.out.println("Sent message, The response is...");
                    ByteBuffer recvBuf = ByteBuffer.allocate(128);
                    // Now receive and print the whole response
                    int rec = Channels.readBlocking(connection.getSourceChannel(), recvBuf);
                    if (rec > 0) {
                        recvBuf.flip();
                        final CharBuffer chars = charset.decode(recvBuf);
                        System.out.println(chars);
                        recvBuf.clear();
//                    connection.close();
                    }
                    if (rec == -1) {
                        System.out.println("Closing");
                        connection.close();
                    }
                }

                Thread.sleep(32000);

            } finally {
                IoUtils.safeClose(connection);
            }
        } finally {
            worker.shutdown();
        }
    }
}
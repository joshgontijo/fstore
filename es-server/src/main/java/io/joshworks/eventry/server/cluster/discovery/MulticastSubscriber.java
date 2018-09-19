package io.joshworks.eventry.server.cluster.discovery;

import io.joshworks.fstore.core.io.IOUtils;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.util.concurrent.atomic.AtomicBoolean;

public class MulticastSubscriber implements Runnable, Closeable {

    private final DatagramChannel channel;
    private final InetAddress group;
    private final NetworkInterface networkInterface;
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(4096);
    private final AtomicBoolean stopped = new AtomicBoolean();


    public static void main(String[] args) {
        new Thread(new MulticastSubscriber("224.0.0.3", 5555)).start();
        new Thread(new MulticastPublisher("224.0.0.3", 5555)).start();
    }

    public MulticastSubscriber(String address, int port) {
        try {
            this.networkInterface = NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"));
            this.group = InetAddress.getByName(address);
            this.channel = DatagramChannel.open(StandardProtocolFamily.INET)
                    .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                    .bind(new InetSocketAddress(port))
                    .setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void run() {
        try {
            MembershipKey key = channel.join(group, networkInterface);
            while (!stopped.get()) {
                if (key.isValid()) {
                    byteBuffer.clear();
                    InetSocketAddress sa = (InetSocketAddress) channel.receive(byteBuffer);

                    byteBuffer.flip();
                    String message = new String(byteBuffer.array(), 0, byteBuffer.limit());
                    System.out.println("Multicast received from " + sa.getHostName() + ": " + message);

                    channel.send(ByteBuffer.wrap(message.getBytes()), sa);

                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if(!stopped.compareAndSet(false, true)) {
            return;
        }
        IOUtils.closeQuietly(channel);
    }
}
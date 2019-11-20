package io.joshworks.fstore.server.cluster_old.discovery;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class MulticastPublisher implements Runnable {

    private final String address;
    private final int port;

//    final static String INET_ADDR = "224.0.0.3";
//    final static int PORT = 8888;


    public MulticastPublisher(String address, int port) {
        this.address = address;
        this.port = port;
    }


    @Override
    public void run() {

        try (DatagramChannel channel = DatagramChannel.open()) {
//            channel.socket().bind(new InetSocketAddress(port));

            InetSocketAddress inetSocketAddress = new InetSocketAddress(address, port);
            for (int i = 0; i < 5; i++) {
                String message = "Some message " + System.currentTimeMillis();

                System.out.println("Sending: " + message);

                ByteBuffer buf = ByteBuffer.allocate(48);
                buf.put(message.getBytes());
                buf.flip();

                int bytesSent = channel.send(buf, inetSocketAddress);


                ByteBuffer read = ByteBuffer.allocate(48);
                InetSocketAddress sa = (InetSocketAddress) channel.receive(read);

                read.flip();
                String readMsg = new String(read.array(), 0, read.limit());
                System.out.println("Got Response: " + readMsg);


                Thread.sleep(500);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
package io.joshworks.eventry.server.cluster_old.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NioClient {

    public static void main(String[] args) throws IOException, InterruptedException {

        InetSocketAddress address = new InetSocketAddress("localhost", 9090);
        try (SocketChannel client = SocketChannel.open(address)) {
            byte[] message = "Hi !".getBytes();

            System.out.println("Sending data");
            client.write(ByteBuffer.wrap(message));




//            ByteBuffer read = ByteBuffer.allocate(4096);
//            client.read(read);
//
//            System.out.println("Response: " + new String(read.array(), 0, read.position()));

        }
    }
}
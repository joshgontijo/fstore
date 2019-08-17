package io.joshworks.eventry.network.tcp_xnio;

import io.joshworks.eventry.server.tcp_xnio.tcp.TcpConnection;
import io.joshworks.eventry.server.tcp_xnio.tcp.TcpEventClient;
import io.joshworks.eventry.server.tcp_xnio.tcp.example.Address;
import io.joshworks.eventry.server.tcp_xnio.tcp.example.User;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public final class EchoClient {

    public static void main(String[] args) throws Exception {

        TcpConnection connection = TcpEventClient.create()
                .onClose(conn -> System.out.println("Disconnected from " + conn.peerAddress()))
                .keepAlive(2, TimeUnit.SECONDS)
                .registerTypes(User.class, Address.class)
                .onEvent((conn, data) -> {
                    System.out.println("Received: " + data);
                })
                .connect(new InetSocketAddress("127.0.0.1", 10000), 5, TimeUnit.SECONDS);


    }
}
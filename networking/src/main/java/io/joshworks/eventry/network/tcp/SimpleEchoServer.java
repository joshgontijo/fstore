//package io.joshworks.eventry.network.tcp;
//
//import io.joshworks.eventry.server.tcp_xnio.tcp.XTcpServer;
//import io.joshworks.eventry.server.tcp_xnio.tcp.example.Address;
//import io.joshworks.eventry.server.tcp_xnio.tcp.example.User;
//import io.joshworks.eventry.server.tcp_xnio.tcp.internal.KeepAlive;
//
//import java.net.InetSocketAddress;
//
//public final class SimpleEchoServer {
//
//    public static void main(String[] args) {
//
//        XTcpServer server = XTcpServer.create()
////                .idleTimeout(5, TimeUnit.SECONDS)
//                .onOpen(tcp -> System.out.println("OPEN " + tcp.peerAddress()))
//                .onClose(tcp -> System.out.println("CLOSE " + tcp.peerAddress()))
//                .onIdle(tcp -> System.out.println("IDLE " + tcp.peerAddress()))
////                .onEvent((connection, data) -> {
////                    String msg = Serializers.STRING.fromBytes((ByteBuffer) data);
////                    ByteBuffer rsp = ByteBuffer.allocate(128);
////                    Serializers.STRING.writeTo("ECHO: " + msg, rsp);
////                    rsp.flip();
////                    connection.send(rsp);
////                })
//                .onEvent((connection, data) -> {
//                    if(data instanceof KeepAlive) {
//                        System.out.println("Received keep alive");
//                    }
//                    if (data instanceof User) {
//                        System.out.println("USER -> " + data);
//                    }
//                    if (data instanceof Address) {
//                        System.out.println("ADDRESS -> " + data);
//                    }
//                })
//                .start(new InetSocketAddress("127.0.0.1", 10000));
//
//    }
//
//}
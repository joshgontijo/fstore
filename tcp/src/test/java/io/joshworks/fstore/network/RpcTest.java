package io.joshworks.fstore.network;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.TcpMessageServer;
import io.joshworks.fstore.tcp.client.TcpEventClient;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.server.TypedEventHandler;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RpcTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) {

        TcpMessageServer server = TcpMessageServer.create()
                .onOpen(conn -> System.out.println("SERVER: Connection opened"))
                .onClose(conn -> System.out.println("SERVER: Connection closed"))
                .onIdle(conn -> System.out.println("SERVER: Connection idle"))
                .idleTimeout(10, TimeUnit.SECONDS)
                .maxEntrySize(Size.KB.ofInt(64))
                .asyncHandler()
                .onEvent(TypedEventHandler.rpcHandler(new RpcHandler()))
                .start(new InetSocketAddress(HOST, PORT));


        TcpClientConnection client = TcpEventClient.create()
                .option(Options.WORKER_NAME, "CLIENT-" + UUID.randomUUID().toString().substring(0, 3))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(32))
                .bufferSize(Size.KB.ofInt(32))
                .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);


        Response<Object> response = client.invoke("doSomething");
        System.out.println(response.get()); //null

        Response<String> response1 = client.invoke("returnSomething");
        System.out.println(response1.get()); //yolo

        Response<String> response2 = client.invoke("echo", "Ola !");
        System.out.println(response2.get()); //ola

        IRpcHandler rpcProxy = client.createRpcProxy(IRpcHandler.class, 3000, false);

        String echo = rpcProxy.echo("Yolo !!!!");
        System.out.println(echo);

        try {
            rpcProxy.exception();
        } catch (Exception e) {
            System.err.println("FAILED WITH: " + e.getMessage());
        }


        System.out.println("CLOSING SERVER");
        client.close();
        server.close();
    }


    private static class RpcHandler implements IRpcHandler {

        @Override
        public void doSomething() {
            System.out.println("doSomething");
        }

        @Override
        public String returnSomething() {
            System.out.println("returnSomething");
            return "YOLO";
        }

        @Override
        public String echo(String param) {
            System.out.println("echo: " + param);
            return param;
        }

        @Override
        public String exception() {
            throw new RuntimeException("ERROR !!!");
        }

    }

}

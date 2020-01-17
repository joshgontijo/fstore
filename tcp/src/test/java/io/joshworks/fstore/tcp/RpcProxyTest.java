package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.handlers.TypedEventHandler;
import io.joshworks.fstore.tcp.internal.Response;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class RpcProxyTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) {

        TcpEventServer server = TcpEventServer.create()
                .onEvent(TypedEventHandler.builder().registerRpc(new RpcHandler()).build())
                .start(new InetSocketAddress(HOST, PORT));


        TcpConnection client = TcpEventClient.create()
                .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                .onEvent(TypedEventHandler.builder().build())
                .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);


        Response<Object> response = client.invoke("doSomething");
        System.out.println(response.get()); //null

        Response<String> response1 = client.invoke("returnSomething");
        System.out.println(response1.get()); //yolo

        Response<String> response2 = client.invoke("echo", "Ola !");
        System.out.println(response2.get()); //ola

        RpcProxy rpcProxy = client.createRpcProxy(RpcProxy.class, 3000, false);

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


    private static class RpcHandler implements RpcProxy {

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

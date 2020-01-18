package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.tcp.internal.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RpcProxyTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    private TcpEventServer server;
    private TcpConnection client;

    private Set<String> calledMethods = new HashSet<>();

    @Before
    public void setUp() {
        server = TcpEventServer.create()
                .onEvent(TypedEventHandler.builder().registerRpc(new RpcHandler()).build())
                .start(new InetSocketAddress(HOST, PORT));

        client = TcpEventClient.create()
                .onEvent(TypedEventHandler.builder().build())
                .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(server);
        IOUtils.closeQuietly(client);
        calledMethods.clear();
    }

    @Test
    public void invoke() {
        var methodName = "doSomething";
        Response<Object> response = client.invoke(methodName);
        response.get();
        assertTrue(calledMethods.contains(methodName));
    }

    @Test
    public void invokeWithReturnType() {
        var methodName = "returnSomething";
        Response<String> response = client.invoke(methodName);
        String resp = response.get();
        assertEquals(RpcHandler.SOME_MESSAGE, resp);
        assertTrue(calledMethods.contains(methodName));
    }

    @Test
    public void invoke_with_param_and_return_value() {
        var methodName = "echo";
        var message = "Ola !";
        Response<String> response = client.invoke(methodName, message);
        assertEquals(message, response.get());
        assertTrue(calledMethods.contains(methodName));
    }

    @Test(expected = RuntimeException.class)
    public void invoke_exception() {
        var methodName = "exception";
        Response<String> response = client.invoke(methodName);
        response.get();
        assertTrue(calledMethods.contains(methodName));
    }

    @Test
    public void rpcProxy() {
        RpcProxy rpc = client.createRpcProxy(RpcProxy.class, 20000);
        String message = "yolo";
        String resp = rpc.echo(message);
        assertEquals(message, resp);
        assertTrue(calledMethods.contains("echo"));
    }

    private class RpcHandler implements RpcProxy {

        public static final String SOME_MESSAGE = "YOLO";

        @Override
        public void doSomething() {
            calledMethods.add("doSomething");
        }

        @Override
        public String returnSomething() {
            calledMethods.add("returnSomething");
            return SOME_MESSAGE;
        }

        @Override
        public String echo(String param) {
            calledMethods.add("echo");
            return param;
        }

        @Override
        public String exception() {
            calledMethods.add("exception");
            throw new RuntimeException("ERROR !!!");
        }

    }

}

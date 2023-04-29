package io.joshworks.fstore.cluster.rpc;

import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.ClusterClientException;
import io.joshworks.fstore.cluster.LoggingInterceptor;
import io.joshworks.fstore.core.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RpcClientTest {

    private static final String SOME_VALUE = "YOLO";

    private String cluster = "test-cluster";
    private String node1Id = "node-1";
    private String node2Id = "node-2";

    private Cluster cluster1;
    private Cluster cluster2;

    @Before
    public void setUp() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");

        cluster1 = new Cluster(cluster, node1Id);
        cluster2 = new Cluster(cluster, node2Id);

        cluster1.interceptor(new LoggingInterceptor());
        cluster1.registerRpcHandler(new RpcTestReceiver());


        cluster1.join();
        cluster2.join();
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(cluster1);
        IOUtils.closeQuietly(cluster2);
    }

    @Test
    public void invokeAsync() {
        cluster2.rpcClient().invokeAsync(cluster1.address(), "doSomething");
    }

    @Test
    public void invoke_null_response() {
        Object resp = cluster2.rpcClient().invoke(cluster1.address(), "doSomething");
        assertNull(resp);
    }

    @Test
    public void invoke_with_response() {
        String resp = cluster2.rpcClient().invoke(cluster1.address(), "justReturn");
        assertEquals(SOME_VALUE, resp);
    }

    @Test
    public void invoke_with_echo_response() {
        String resp = cluster2.rpcClient().invoke(cluster1.address(), "echo", new Object[]{SOME_VALUE});
        assertEquals(SOME_VALUE, resp);
    }

    @Test(expected = ClusterClientException.class)
    public void exception() {
        String resp = cluster2.rpcClient().invoke(cluster1.address(), "exception");
        System.out.println(resp);
    }

    @Test
    public void invokeWithFuture() {
    }

    @Test
    public void invokeAllWithFuture() {
    }

    @Test
    public void invokeAll() {
    }

    public static class RpcTestReceiver {

        public void doSomething() {
            System.out.println("doSomething");
        }

        public String echo(String param) {
            System.out.println("echo: " + param);
            return param;
        }

        public String justReturn() {
            System.out.println("justReturn");
            return SOME_VALUE;
        }

        public void exception() {
            throw new RuntimeException("SOME ERROR");
        }

    }

}
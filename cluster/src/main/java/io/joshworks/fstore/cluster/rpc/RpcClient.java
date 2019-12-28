package io.joshworks.fstore.cluster.rpc;

import io.joshworks.fstore.cluster.ClusterClient;
import io.joshworks.fstore.cluster.ClusterClientException;
import org.jgroups.Address;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RpcClient {

    private final ClusterClient client;

    public RpcClient(ClusterClient client) {
        this.client = client;
    }

    public void invokeAsync(Address address, String method, Object... params) {
        RpcMessage message = new RpcMessage(method, params);
        client.sendAsync(address, message);
    }

    public <T> T invoke(Address address, String method, Object... params) {
        RpcMessage message = new RpcMessage(method, params);
        return client.send(address, message);
    }

    public <T> CompletableFuture<T> invokeWithFuture(Address address, String method, Object... params) {
        RpcMessage message = new RpcMessage(method, params);
        return client.sendWithFuture(address, message);
    }

    public <T> List<CompletableFuture<T>> invokeAllWithFuture(List<Address> addresses, String method, Object... params) {
        RpcMessage message = new RpcMessage(method, params);

        List<CompletableFuture<T>> calls = new ArrayList<>(addresses.size());
        for (Address address : addresses) {
            calls.add(client.sendWithFuture(address, message));
        }

        return calls;
    }

    /**
     * All or nothing
     */
    public <T> List<T> invokeAll(List<Address> addresses, String method, Object... params) {
        List<CompletableFuture<T>> calls = invokeAllWithFuture(addresses, method, params);
        List<T> responses = new ArrayList<>();
        try {
            for (CompletableFuture<T> call : calls) {
                responses.add(call.get());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return responses;
    }

    public <T> T createProxy(Class<T> type, Address address, int timeoutMillis) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class[]{type},
                new RpcProxyHandler(address, timeoutMillis));
    }

    private class RpcProxyHandler implements InvocationHandler {

        private final int timeoutMillis;
        private final Address target;

        private RpcProxyHandler(Address target, int timeoutMillis) {
            this.target = target;
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            if (Void.TYPE.equals(method.getReturnType())) {
                invokeAsync(target, methodName, args);
                return null;
            }
            if (Future.class.equals(method.getReturnType())) {
                return RpcClient.this.invokeWithFuture(target, methodName, args);
            }
            if (CompletableFuture.class.equals(method.getReturnType())) {
                return RpcClient.this.invokeWithFuture(target, methodName, args);
            }
            try {
                return RpcClient.this.invokeWithFuture(target, methodName, args).get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new ClusterClientException("Failed to invoke " + methodName + " on " + target, e);
            }
        }
    }

}

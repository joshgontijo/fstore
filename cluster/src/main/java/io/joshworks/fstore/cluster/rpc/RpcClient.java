package io.joshworks.fstore.cluster.rpc;

import io.joshworks.fstore.cluster.ClusterClient;
import org.jgroups.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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


}

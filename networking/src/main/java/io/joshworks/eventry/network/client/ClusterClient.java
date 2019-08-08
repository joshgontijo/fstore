package io.joshworks.eventry.network.client;

import io.joshworks.eventry.network.MessageError;
import io.joshworks.eventry.network.MulticastResponse;
import io.joshworks.eventry.network.NullMessage;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.jgroups.Address;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;

import static java.util.Objects.requireNonNull;

public class ClusterClient {

    private final MessageDispatcher dispatcher;
    private final LockService lockService;
    private final ExecutionService executionService;

    public ClusterClient(MessageDispatcher dispatcher, LockService lockService, ExecutionService executionService) {
        this.dispatcher = dispatcher;
        this.lockService = lockService;
        this.executionService = executionService;
    }

    public ExecutorService executor() {
        return executionService;
    }

    public Lock lock(String name) {
        return lockService.getLock(name);
    }

    /**
     * Sends a synchronous request and wait for a response
     */
    public <T> T send(Address address, Object message) {
        byte[] response = send(address, message, RequestOptions.SYNC());
        if (response == null) {
            return null;
        }

        Object cMessage = KryoStoreSerializer.deserialize(response);
        if (cMessage instanceof MessageError) {
            throw new RuntimeException("TODO " + cMessage);
        }
        if (cMessage instanceof NullMessage) {
            return null;
        }
        return (T) cMessage;
    }

    /**
     * Fire and forget, no response expected from the target node
     */
    public void sendAsync(Address address, Object message) {
        send(address, message, RequestOptions.ASYNC());
    }

    /**
     * Sends a synchronous request and wait for all responses
     */
    public List<MulticastResponse> cast(Object message) {
        return cast(null, message);
    }

    /**
     * Sends a synchronous request to the specified nodes and wait for all responses, responses must not be null,
     */
    public List<MulticastResponse> cast(List<Address> addresses, Object message) {
        RspList<byte[]> responses = cast(addresses, message, RequestOptions.SYNC());
        requireNonNull(responses, "Responses cannot be null");

        List<MulticastResponse> nodeResponses = new ArrayList<>();
        for (Rsp<byte[]> response : responses) {
            requireNonNull(response, "Response cannot be null");
            byte[] respMsg = response.getValue();
            if (respMsg != null) {
                Object cm = KryoStoreSerializer.deserialize(respMsg);
                nodeResponses.add(new MulticastResponse(null, cm));
            }

        }
        return nodeResponses;
    }

    /**
     * Sends a asynchronous request to all nodes in the cluster
     */
    public void castAsync(Object message) {
        castAsync(null, message);
    }

    /**
     * Sends a asynchronous request to the specified nodes in the cluster
     */
    public void castAsync(List<Address> addresses, Object message) {
        cast(addresses, message, RequestOptions.ASYNC());
    }

    private byte[] send(Address address, Object message, RequestOptions options) {
        try {
            byte[] data = KryoStoreSerializer.serialize(message);
            Object o = dispatcher.sendMessage(address, new Buffer(data), options);
            return (byte[]) o;
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + address + ": ", e);
        }
    }

    private RspList<byte[]> cast(Collection<Address> addresses, Object message, RequestOptions options) {
        try {
            byte[] data = KryoStoreSerializer.serialize(message);
            return dispatcher.castMessage(addresses, new Buffer(data), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + Arrays.toString(addresses.toArray()) + ": ", e);
        }
    }
}

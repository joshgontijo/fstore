package io.joshworks.eventry.server.cluster.client;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ClusterClient {

    private final MessageDispatcher dispatcher;

    public ClusterClient(MessageDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * Sends a synchronous request and wait for a response
     */
    public NodeMessage send(Address address, ClusterMessage message) {
        Message response = send(address, message, RequestOptions.SYNC());
        return response == null ? null : new NodeMessage(response);
    }

    /**
     * Fire and forget, no response expected from the target node
     */
    public void sendAsync(Address address, ClusterMessage message) {
        send(address, message, RequestOptions.ASYNC());
    }

    /**
     * Sends a synchronous request and wait for all responses
     */
    public List<NodeMessage> cast(ClusterMessage message) {
        return cast(null, message);
    }

    /**
     * Sends a synchronous request to the specified nodes and wait for all responses, responses must not be null,
     */
    public List<NodeMessage> cast(List<Address> addresses, ClusterMessage message) {
        RspList<Message> responses = cast(addresses, message, RequestOptions.SYNC());
        requireNonNull(responses, "Responses cannot be null");

        List<NodeMessage> nodeResponses = new ArrayList<>();
        for (Rsp<Message> response : responses) {
            requireNonNull(response, "Response cannot be null");
            Message respMsg = response.getValue();
            nodeResponses.add(new NodeMessage(respMsg));
        }
        return nodeResponses;
    }

    /**
     * Sends a asynchronous request to all nodes in the cluster
     */
    public void castAsync(ClusterMessage message) {
        castAsync(null, message);
    }

    /**
     * Sends a asynchronous request to the specified nodes in the cluster
     */
    public void castAsync(List<Address> addresses, ClusterMessage message) {
        cast(addresses, message, RequestOptions.ASYNC());
    }

    private Message send(Address address, ClusterMessage message, RequestOptions options) {
        try {
            return dispatcher.sendMessage(address, new Buffer(message.toBytes()), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + address + ": ", e);
        }
    }

    private RspList<Message> cast(Collection<Address> addresses, ClusterMessage message, RequestOptions options) {
        try {
            return dispatcher.castMessage(addresses, new Buffer(message.toBytes()), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + Arrays.toString(addresses.toArray()) + ": ", e);
        }
    }
}

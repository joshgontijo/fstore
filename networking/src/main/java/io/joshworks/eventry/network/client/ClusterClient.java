package io.joshworks.eventry.network.client;

import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.eventry.network.MessageError;
import io.joshworks.eventry.network.NodeMessage;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ClusterClient {

    private final MessageDispatcher dispatcher;
    private final KryoStoreSerializer serializer;

    public ClusterClient(MessageDispatcher dispatcher, KryoStoreSerializer serializer) {
        this.dispatcher = dispatcher;
        this.serializer = serializer;
    }

    /**
     * Sends a synchronous request and wait for a response
     */
    public <T extends ClusterMessage> T send(Address address, ClusterMessage message) {
        Message response = send(address, message, RequestOptions.SYNC());
        if(response == null) {
            return null;
        }

        ClusterMessage cMessage = (ClusterMessage) serializer.fromBytes(ByteBuffer.wrap(response.buffer()));
        if(cMessage instanceof MessageError) {
            throw new RuntimeException("TODO " + cMessage);
        }
        return (T) cMessage;
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
            ClusterMessage cm = (ClusterMessage) serializer.fromBytes(ByteBuffer.wrap(respMsg.buffer()));
            nodeResponses.add(new NodeMessage(respMsg.src(), cm));
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
            return dispatcher.sendMessage(address, new Buffer(serializer.toBytes(message).array()), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + address + ": ", e);
        }
    }

    private RspList<Message> cast(Collection<Address> addresses, ClusterMessage message, RequestOptions options) {
        try {
            return dispatcher.castMessage(addresses, new Buffer(serializer.toBytes(message).array()), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + Arrays.toString(addresses.toArray()) + ": ", e);
        }
    }
}

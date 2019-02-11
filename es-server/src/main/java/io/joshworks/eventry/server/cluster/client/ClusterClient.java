package io.joshworks.eventry.server.cluster.client;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ClusterClient {

    private final MessageDispatcher dispatcher;
    private final AddressMapper addressMapper;

    public ClusterClient(MessageDispatcher dispatcher, AddressMapper addressMapper) {
        this.dispatcher = dispatcher;
        this.addressMapper = addressMapper;
    }

    public NodeResponse send(String uuid, ClusterMessage message) {
        Message response = send(uuid, message, RequestOptions.SYNC());
        requireNonNull(response, "Response cannot be null");
        return new NodeResponse(uuid, ByteBuffer.wrap(response.buffer()));
    }

    public void sendAsync(String uuid, ClusterMessage message) {
        send(uuid, message, RequestOptions.ASYNC());
    }

    public ClusterResponse cast(ClusterMessage message) {
        return cast(null, message);
    }

    public ClusterResponse cast(List<String> uuids, ClusterMessage message) {
        RspList<Message> responses = cast(uuids, message, RequestOptions.SYNC());
        requireNonNull(responses, "Responses cannot be null");

        List<NodeResponse> nodeResponses = new ArrayList<>();
        for (Rsp<Message> response : responses) {
            requireNonNull(response, "Response cannot be null");
            Message respMsg = response.getValue();
            ByteBuffer resp = ByteBuffer.wrap(respMsg.buffer());
            String uuid = addressMapper.toUuid(respMsg.src());
            nodeResponses.add(new NodeResponse(uuid, resp));
        }
        return new ClusterResponse(nodeResponses);
    }

    public void castAsync(ClusterMessage message) {
        castAsync(null, message);
    }

    public void castAsync(List<String> uuids, ClusterMessage message) {
        cast(uuids, message, RequestOptions.ASYNC());
    }

    private Message send(String uuid, ClusterMessage message, RequestOptions options) {
        Address address = addressMapper.toAddress(uuid);
        try {
            return dispatcher.sendMessage(address, new Buffer(message.toBytes()), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + address + ": ", e);
        }
    }

    private RspList<Message> cast(Collection<String> uuids, ClusterMessage message, RequestOptions options) {
        List<Address> addresses = uuids == null ? null : uuids.stream().map(addressMapper::toAddress).collect(Collectors.toList());
        try {
            return dispatcher.castMessage(addresses, new Buffer(message.toBytes()), options);
        } catch (Exception e) {
            throw new ClusterClientException("Failed sending message to " + Arrays.toString(addresses.toArray()) + ": ", e);
        }
    }
}

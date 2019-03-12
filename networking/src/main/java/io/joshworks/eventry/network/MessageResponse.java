package io.joshworks.eventry.network;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.Response;

import java.nio.ByteBuffer;

public class MessageResponse {



    public MessageResponse(Address dst, Address src, Response response, KryoStoreSerializer serializer) {
        this.dst = src;
        this.src = src;
        this.response = response;
        this.serializer = serializer;
    }

    public void send(ClusterMessage responseMsg) {
        if(responseMsg == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        ByteBuffer data = serializer.toBytes(responseMsg);
        response.send(new Message(dst, data.array()).setSrc(src), false);
        sent = true;
    }

}

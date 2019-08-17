package io.joshworks.fstore.es.shared.tcp_xnio.tcp;

import io.joshworks.eventry.server.tcp_xnio.tcp.internal.KeepAlive;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.nio.ByteBuffer;

class KryoEventHandler implements EventHandler {

    private final EventHandler delegate;



    KryoEventHandler(EventHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        Object obj;
        try {
            ByteBuffer buff = (ByteBuffer) data;
            obj = KryoStoreSerializer.deserialize(buff);
            if(obj instanceof KeepAlive) {
                //TODO log ?
                return;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while parsing data", e);
        }
        delegate.onEvent(connection, obj);
    }
}

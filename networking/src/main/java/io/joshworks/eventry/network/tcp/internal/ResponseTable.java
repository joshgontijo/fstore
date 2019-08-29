package io.joshworks.eventry.network.tcp.internal;

import io.joshworks.eventry.network.tcp.LengthPrefixCodec;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ResponseTable {

    private final Map<Long, Response> table = new ConcurrentHashMap<>();
    private final AtomicLong reqids = new AtomicLong();

    //can cause build up heap usage if Response#get is not used
    public <T> Response<T> newRequest(Object data, ByteBuffer buffer) {
        long reqId = reqids.getAndIncrement();
        Message message = new Message(reqId, data);
        LengthPrefixCodec.serialize(message, buffer);
        return new Response<>(reqId, table::remove);
    }

    public Response complete(long id) {
        return table.remove(id);
    }

    public void clear() {
        table.clear();
    }
}

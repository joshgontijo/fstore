package io.joshworks.eventry.network.tcp;


import io.joshworks.eventry.network.tcp.internal.Message;
import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.xnio.StreamConnection;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class TcpClientConnection extends TcpConnection {

    private final Map<Long, Response> responseTable;
    private final AtomicLong reqids = new AtomicLong();

    public TcpClientConnection(StreamConnection connection, BufferPool writePool, Map<Long, Response> responseTable) {
        super(connection, writePool);
        this.responseTable = responseTable;
    }

    public <T, R> Response<R> request(T data) {
        requireNonNull(data, "Entity must be provided");

        try (writePool) {
            long reqId = reqids.getAndIncrement();
            ByteBuffer buffer = writePool.allocate();
            Message message = new Message(reqId, data);
            LengthPrefixCodec.serialize(message, buffer);
            buffer.flip();

            Response<R> response = new Response<>(reqId, responseTable::remove);
            responseTable.put(reqId, response);

            super.write(buffer, false);
            return response;
        }
    }

}

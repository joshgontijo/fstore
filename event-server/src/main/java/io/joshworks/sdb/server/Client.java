package io.joshworks.sdb.server;

import io.joshworks.EventClient;
import io.joshworks.es2.StreamHasher;
import io.joshworks.fstore.core.util.StringUtils;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.joshworks.es2.Event.HEADER_BYTES;
import static io.joshworks.es2.Event.NO_VERSION;
import static io.joshworks.fstore.core.util.StringUtils.toUtf8Bytes;

public class Client {

    private final EventClient client;

    public static Client connect(String host, int port) {
        return new Client(EventClient
                .create()
                .connect(host, port));
    }

    private Client(EventClient client) {
        this.client = client;
    }

    public StreamIterator subscribe(String stream, int fromVersionInclusive) {

    }


    public CompletableFuture<Integer> append(String stream, String eventType, Object event) {
        return append(stream, eventType, NO_VERSION, event);
    }

    public CompletableFuture<Integer> append(String stream, String eventType, int version, Object event) {
        client.send(serialize(stream, eventType, version, event));
        return CompletableFuture.completedFuture(0);
    }

    public int version(String stream) {
        return append(stream, eventType, NO_VERSION, event);
    }


}

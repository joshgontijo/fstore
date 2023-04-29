package io.joshworks.sdb.http;


import io.joshworks.es2.Event;
import io.joshworks.es2.EventStore;
import io.joshworks.es2.StreamHasher;
import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.StreamBlock;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.StringUtils;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.snappy.http.Response;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.joshworks.snappy.SnappyServer.cors;
import static io.joshworks.snappy.SnappyServer.get;
import static io.joshworks.snappy.SnappyServer.post;
import static io.joshworks.snappy.SnappyServer.start;
import static io.joshworks.snappy.http.Response.ok;

public class Main {

    public static void main(String[] args) throws IOException {

        var path = Path.of("store");
        TestUtils.deleteRecursively(path.toFile());
        Files.createDirectory(path);
        var store = EventStore.open(path).build();

        cors();

        post("/streams/{stream}", req -> {
            var body = req.body().asByteArray();

            String stream = req.pathParameter("stream");
            String evType = req.header("Event-Type");
            String versionStr = req.header("Version");
            int version = versionStr == null ? -1 : Integer.parseInt(versionStr);

            var evBuffer = composeEvent(body, evType, version, stream);
            CompletableFuture<Integer> task = store.append(evBuffer);
            int eventVersion = task.join();
            return ok(Map.of("version", eventVersion));
        });

        get("/streams/{stream}", req -> {
            String stream = req.pathParameter("stream");
            int version = req.queryParameterVal("version").asInt().orElse(0);

            Sink.Memory sink = new Sink.Memory();

            int read = store.read(StreamHasher.hash(stream), version, sink);

            return Response.ok(parseEvents(ByteBuffer.wrap(sink.data())));
        });

        get("/streams/{stream}/version", req -> {
            String stream = req.pathParameter("stream");
            int version = store.version(StreamHasher.hash(stream));
            return Response.ok(Map.of("version", version));
        });

        start();
        store.close();


    }

    private static List<HttpEvent> parseEvents(ByteBuffer chunk) {
        var decompressedSize = StreamBlock.uncompressedSize(chunk);
        var dst = Buffers.allocate(decompressedSize, false);
        StreamBlock.decompress(chunk, dst);
        dst.flip();

        List<HttpEvent> events = new ArrayList<>();
        while (dst.hasRemaining()) {
            int recLen = Event.sizeOf(dst);
            int version = Event.version(dst);
            long timestamp = Event.timestamp(dst);
            String type = Event.eventType(dst);
            Map<String, Object> data = JsonSerializer.toMap(new String(Event.data(dst), StandardCharsets.UTF_8));
            events.add(new HttpEvent(version, timestamp, type, data));
            Buffers.offsetPosition(dst, recLen);
        }
        return events;
    }

    private static ByteBuffer composeEvent(byte[] data, String type, int version, String stream) {
        byte[] typeBytes = StringUtils.toUtf8Bytes(type);
        int recSize = Event.HEADER_BYTES + typeBytes.length + data.length;

        ByteBuffer dst = ByteBuffer.allocate(recSize);
        int bpos = dst.position();

        dst.putInt(recSize);
        dst.putLong(StreamHasher.hash(stream));
        dst.putInt(version);

        long ts = System.currentTimeMillis();
        dst.putLong(ts);

        dst.putShort((short) typeBytes.length);
        dst.putInt(data.length);
        dst.put(typeBytes);
        dst.put(data);

        int copied = (dst.position() - bpos);

        assert copied == recSize;
        return dst.flip();
    }

}

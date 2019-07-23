package io.joshworks.eventry.stream;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StreamMetadataSerializer implements Serializer<StreamMetadata> {

    private final Serializer<Map<String, Integer>> permissionSerializer = Serializers.mapSerializer(Serializers.VSTRING, Serializers.INTEGER, ConcurrentHashMap::new);
    private final Serializer<Map<String, String>> metadataSerializer = Serializers.mapSerializer(Serializers.VSTRING, Serializers.VSTRING, ConcurrentHashMap::new);


    @Override
    public void writeTo(StreamMetadata data, ByteBuffer dst) {
        dst.putLong(data.hash);
        dst.putLong(data.created);
        dst.putLong(data.maxAge);
        dst.putInt(data.maxCount);
        dst.putInt(data.state);
        dst.putInt(data.truncated);
        Serializers.VSTRING.writeTo(data.name, dst);

        permissionSerializer.writeTo(data.acl, dst);
        metadataSerializer.writeTo(data.metadata, dst);
    }

    @Override
    public StreamMetadata fromBytes(ByteBuffer buffer) {
        long hash = buffer.getLong();
        long created = buffer.getLong();
        long maxAge = buffer.getLong();
        int maxCount = buffer.getInt();
        int state = buffer.getInt();
        int truncateBefore = buffer.getInt();
        String name = Serializers.VSTRING.fromBytes(buffer);

        Map<String, Integer> permissions = permissionSerializer.fromBytes(buffer);
        Map<String, String> metadata = metadataSerializer.fromBytes(buffer);

        return new StreamMetadata(name, hash, created, maxAge, maxCount, truncateBefore, permissions, metadata, state);
    }

}

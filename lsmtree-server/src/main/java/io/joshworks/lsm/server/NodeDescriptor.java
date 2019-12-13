package io.joshworks.lsm.server;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.extra.DataFile;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.collection.EntrySerializer;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


public class NodeDescriptor implements Closeable {

    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String NODE_ID = "NODE_ID";

    private static final String FILE = ".node";

    private final Map<String, String> data = new HashMap<>();
    private final DataFile<Map.Entry<String, String>> dataFile;

    private NodeDescriptor(DataFile<Map.Entry<String, String>> dataFile) {
        this.dataFile = dataFile;
    }

    public static synchronized NodeDescriptor read(File root) {
        try {
            File pFile = new File(root, FILE);
            boolean exists = Files.exists(pFile.toPath());
            if (!exists) {
                return null;
            }
            if (!Files.exists(pFile.toPath())) {
                return null;
            }

            var dataFile = DataFile.of(new EntrySerializer<>(Serializers.VSTRING, Serializers.VSTRING)).open(new File(root, FILE));
            NodeDescriptor descriptor = new NodeDescriptor(dataFile);

            Iterator<Map.Entry<String, String>> it = dataFile.iterator(Direction.FORWARD);
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                descriptor.data.put(entry.getKey(), entry.getValue());
            }

            return descriptor;

        } catch (Exception e) {
            throw new RuntimeException("Failed to acquire partition descriptor file", e);
        }
    }

    public static NodeDescriptor create(File root, String clusterName) {

        var dataFile = DataFile.of(new EntrySerializer<>(Serializers.VSTRING, Serializers.VSTRING)).open(new File(root, FILE));
        NodeDescriptor descriptor = new NodeDescriptor(dataFile);

        Map<String, String> data = new HashMap<>();
        data.put(CLUSTER_NAME, clusterName);
        data.put(NODE_ID, UUID.randomUUID().toString().substring(0, 8));

        for (Map.Entry<String, String> kv : data.entrySet()) {
            descriptor.update(kv.getKey(), kv.getValue());
        }
        return descriptor;
    }

    public void update(String key, String value) {
        dataFile.add(new AbstractMap.SimpleEntry<>(key, value));
        data.put(key, value);
    }

    public String nodeId() {
        return data.get(NODE_ID);
    }

    public String clusterName() {
        return data.get(CLUSTER_NAME);
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public Optional<Integer> asInt(String key) {
        return get(key).map(Integer::parseInt);
    }

    public Optional<Long> asLong(String key) {
        return get(key).map(Long::parseLong);
    }

    @Override
    public void close() {
        data.clear();
        dataFile.close();
    }
}

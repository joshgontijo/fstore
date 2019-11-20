package io.joshworks.lsm.server;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * UUID (36 bytes)
 * [NUM_OF_OWNED_PARTITIONS]
 * [OWNED PARTITIONS] (4 byte each)
 */
public class NodeDescriptor implements Closeable {

    private static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String NODE_ID = "NODE_ID";

    private static final String FILE = ".node";
    private final FileChannel channel;
    private final FileLock lock;

    private final Map<String, String> data = new HashMap<>();

    private NodeDescriptor(Map<String, String> data, FileChannel channel, FileLock lock) {
        this.data.putAll(data);
        this.channel = channel;
        this.lock = lock;
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

            FileChannel channel = FileChannel.open(pFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileLock lock = channel.lock();
            try {

                ByteBuffer bb = ByteBuffer.allocate(4096);
                channel.read(bb);
                bb.flip();

                Map<String, String> data = KryoStoreSerializer.deserialize(bb);
                return new NodeDescriptor(data, channel, lock);
            } catch (Exception e) {
                IOUtils.releaseLock(lock);
                IOUtils.closeQuietly(channel);
                throw new RuntimeException("Failed to read node descriptor", e);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to acquire partition descriptor file", e);
        }
    }

    public static NodeDescriptor create(File root, String clusterName) {
        return create(root, clusterName, new HashMap<>());
    }

    public static NodeDescriptor create(File root, String clusterName, Map<String, String> properties) {
        File pFile = new File(root, FILE);
        try {
            Files.createDirectories(root.toPath());
            Files.createFile(pFile.toPath());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write node descriptor", e);
        }

        FileChannel channel = null;
        FileLock lock = null;
        try {
            channel = FileChannel.open(pFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            lock = channel.lock();

            Map<String, String> data = new HashMap<>();
            data.put(CLUSTER_NAME, clusterName);
            data.put(NODE_ID, UUID.randomUUID().toString().substring(0, 8));
            data.putAll(properties);

            byte[] bytes = KryoStoreSerializer.serialize(data);
            NodeDescriptor descriptor = new NodeDescriptor(data, channel, lock);
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
            return descriptor;

        } catch (Exception e) {
            IOUtils.releaseLock(lock);
            IOUtils.closeQuietly(channel);
            throw new RuntimeException("Failed to write node descriptor", e);
        }
    }

    public void update(String key, String value) {
        data.put(key, value);
    }

    public String nodeId() {
        return data.get(NODE_ID);
    }

    public String clusterName() {
        return data.get(CLUSTER_NAME);
    }

    public String get(String key) {
        return data.get(key);
    }

    @Override
    public void close() {
        IOUtils.releaseLock(lock);
        IOUtils.closeQuietly(channel);
    }

    public Integer getInt(String key) {
        String val = get(key);
        if (val == null) {
            return null;
        }
        return Integer.parseInt(val);
    }
}

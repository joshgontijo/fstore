package io.joshworks.eventry.server.cluster;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * UUID (36 bytes)
 * [NUM_OF_OWNED_PARTITIONS]
 * [OWNED PARTITIONS] (4 byte each)
 */
public class NodeDescriptor implements Closeable {

    private static final String FILE = ".node";
    private final FileChannel channel;
    private final FileLock lock;

    private final Data data;

    private NodeDescriptor(Data data, FileChannel channel, FileLock lock) {
        this.data = data;
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

                ByteBuffer bb = ByteBuffer.allocate(1024);
                channel.read(bb);
                bb.flip();

                Data data = KryoStoreSerializer.deserialize(bb, Data.class);
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

    public static NodeDescriptor write(File root, String clusterName, int partitions) {
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

            String uuid = UUID.randomUUID().toString().substring(0, 8);
            Data data = new Data(uuid, clusterName, partitions);
            byte[] bytes = KryoStoreSerializer.serialize(data, Data.class);
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

    public String nodeId() {
        return data.nodeId;
    }

    public String clusterName() {
        return data.clusterName;
    }


    @Override
    public void close() {
        IOUtils.releaseLock(lock);
        IOUtils.closeQuietly(channel);
    }

    public int partitions() {
        return data.partitions;
    }

    private static class Data {
        private final String nodeId;
        private final String clusterName;
        private final int partitions;

        private Data(String id, String clusterName, int partitions) {
            this.nodeId = id;
            this.clusterName = clusterName;
            this.partitions = partitions;
        }
    }

}

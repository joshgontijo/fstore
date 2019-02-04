package io.joshworks.eventry.server.cluster;

import io.joshworks.fstore.core.io.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/**
 * UUID (36 bytes)
 * [NUM_OF_OWNED_PARTITIONS]
 * [OWNED PARTITIONS] (4 byte each)
 */
public class ClusterDescriptor implements Closeable {

    private static final String FILE = ".partitions";
    public final String uuid;
    public final boolean isNew;
    private final FileChannel channel;
    private final FileLock lock;


    private ClusterDescriptor(String uuid, boolean isNew, FileChannel channel, FileLock lock) {
        this.uuid = uuid;
        this.isNew = isNew;
        this.channel = channel;
        this.lock = lock;
    }

    public static synchronized ClusterDescriptor acquire(File root) {
        try {
            File pFile = new File(root, FILE);
            boolean exists = Files.exists(pFile.toPath());
            if(!exists) {
                Files.createDirectories(root.toPath());
            }
            if(!Files.exists(pFile.toPath())) {
                Files.createFile(pFile.toPath());
            }
            FileChannel channel = FileChannel.open(pFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
            FileLock lock = channel.lock();
            String uuid = exists ? readNodeUuid(channel) : writeNodeUuid(channel);
            return new ClusterDescriptor(uuid, !exists, channel, lock);
        } catch (Exception e) {
            throw new RuntimeException("Failed to acquire partition descriptor file", e);
        }
    }

    private static String writeNodeUuid(FileChannel channel) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(36);
        String uuid = UUID.randomUUID().toString();
        byteBuffer.put(uuid.getBytes(StandardCharsets.UTF_8));
        byteBuffer.flip();
        channel.write(byteBuffer);
        channel.force(true);
        return uuid;
    }

    private static String readNodeUuid(FileChannel channel) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(36);
        channel.read(byteBuffer);
        byteBuffer.flip();
        return new String(byteBuffer.array(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        IOUtils.releaseLock(lock);
        IOUtils.closeQuietly(channel);
    }
}

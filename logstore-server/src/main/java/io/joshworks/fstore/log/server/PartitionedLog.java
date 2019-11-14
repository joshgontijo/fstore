package io.joshworks.fstore.log.server;

import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.metrics.MetricRegistry;
import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.naming.SequentialNaming;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PartitionedLog {

    private static final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final List<Partition> partitions = new ArrayList<>();
    private final int numPartitions;
    private final XXHash hasher = new XXHash();

    public PartitionedLog(File root, int numPartitions) throws Exception {
        this.numPartitions = numPartitions;
        Files.createDirectories(root.toPath());

        for (int i = 0; i < numPartitions; i++) {
            partitions.add(openPartition(root, i));
        }
    }

    public void append(String key, byte[] data) {
        int pidx = key == null ? random.nextInt(0, numPartitions) : Math.abs(hasher.hash32(key.getBytes(StandardCharsets.UTF_8))) % numPartitions;
        Partition partition = partitions.get(pidx);
        partition.append(data);
    }

    private static Partition openPartition(File root, int id) {
        String partitionName = "partition-" + id;
        File partitionRoot = new File(root, partitionName);
        ByteBuffer transfer = ByteBuffer.allocate(Size.MB.ofInt(2));
        LogAppender<ByteBuffer> appender = LogAppender.builder(partitionRoot, Serializers.transfer(transfer))
                .segmentSize(Size.MB.of(512))
                .name(partitionName)
                .flushMode(FlushMode.MANUAL)
                .storageMode(StorageMode.RAF)
                .useDirectBufferPool()
                .checksumProbability(1)
                .namingStrategy(new SequentialNaming(partitionRoot))
                .open();
        return new Partition(partitionName, appender);

    }

    private static class Partition {
        private final LogAppender<ByteBuffer> appender;
        private final ExecutorService writeQueue;

        public Partition(String name, LogAppender<ByteBuffer> appender) {
            this.appender = appender;
            this.writeQueue = new MonitoredThreadPool(name, new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new ArrayBlockingQueue<>(100000)));
            MetricRegistry.register(Map.of("partition", name), appender::metrics);
        }

        public void append(byte[] data) {
            writeQueue.execute(() -> appender.append(ByteBuffer.wrap(data)));
        }

    }

}

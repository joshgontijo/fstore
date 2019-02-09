package io.joshworks.eventry.server.cluster.partition;

import io.joshworks.fstore.core.util.Threads;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionTransfer {

    private static final Logger logger = LoggerFactory.getLogger(PartitionTransfer.class);
    private static final int BUFFER_SIZE = 8096;

    private static final ExecutorService executor = Executors.newFixedThreadPool(5, new ThreadFactory() {
        final AtomicLong counter = new AtomicLong();
        @Override
        public Thread newThread(Runnable r) {
            return Threads.named("partition-sender-" + counter.incrementAndGet(), r);
        }
    });
    private final JChannel channel;
    private final PartitionReceiver receiver;

    public PartitionTransfer(File root, JChannel channel) {
        this.channel = channel;
        this.receiver = new PartitionReceiver(root);
        this.channel.setReceiver(receiver);
        ClassConfigurator.add(FileHeader.HEADER_ID, FileHeader.class);
    }

    public CompletableFuture<Void> transferTo(Address dst, File partitionRoot) {
        Address dst1 = channel.getView().getMembers().get(1);
        return CompletableFuture.runAsync(() -> this.transferAll(dst1, partitionRoot), executor);
    }

    private void transferAll(Address dst, File partitionRoot) {
        listFiles(partitionRoot).forEach(file -> {
            String fileName = partitionRoot.toPath().getParent().relativize(file.toPath()).toString();
            transfer(dst, file, fileName);
        });
    }

    private void transfer(Address dst, File file, String fileName) {
        long start = System.currentTimeMillis();
        logger.info("Started transfer of '{}' to {}", fileName, dst);
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] buf = new byte[BUFFER_SIZE]; // think about why not outside the for-loop
            int read;
            long total = 0;
            while ((read = in.read(buf)) != -1) {
                sendMessage(dst, fileName, buf, read, false);
                total += read;
                buf = new byte[8096];
            }
            logger.info("File '{}' of size {} transferred in {}ms", file.getName(), total, (System.currentTimeMillis() - start));

        } catch (Exception e) {
            throw new RuntimeException("Failed to transfer", e);
        } finally {
            sendMessage(dst, fileName, null, 0, true);
        }
    }

    private Collection<File> listFiles(File file) {
        List<File> found = new ArrayList<>();
        if (file.isDirectory()) {
            String[] list = file.list();
            if (list != null) {
                for (String f : list) {
                    File item = new File(file, f);
                    if (item.isDirectory()) {
                        listFiles(item);
                    }
                    found.add(item);
                }
            }
        }
        return found;
    }

    private void sendMessage(Address dst, String fileName, byte[] buf, int length, boolean eof) {
        try {
            Message msg = new Message(dst, buf, 0, length).putHeader(FileHeader.HEADER_ID, new FileHeader(fileName, eof))
                    .setFlag(Message.Flag.DONT_BUNDLE)
                    .setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
            channel.send(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

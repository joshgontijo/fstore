package io.joshworks.eventry.server.cluster.partition;

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

public class PartitionTransfer {

    private static final Logger logger = LoggerFactory.getLogger(PartitionTransfer.class);
    private static final int BUFFER_SIZE = 8096;

    private final JChannel channel;
    private final Address dst;
    private final Collection<File> transferQueue;
    private final File partitionRoot;

    public PartitionTransfer(JChannel channel, Address dst, File partitionRoot) {
        this.channel = channel;
        this.dst = dst;
        this.transferQueue = listFiles(partitionRoot);
        this.partitionRoot = partitionRoot;
        ClassConfigurator.add(FileHeader.HEADER_ID, FileHeader.class);
    }

    public void transfer() {
        transferQueue.forEach(this::transfer);
    }

    private void transfer(File file) {
        long start = System.currentTimeMillis();
        String fileName = partitionRoot.toPath().relativize(file.toPath()).toString();

        logger.info("Started file transfer of '{}'", file.getName());
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] buf = new byte[BUFFER_SIZE]; // think about why not outside the for-loop
            int read;
            long total = 0;
            while ((read = in.read(buf)) != -1) {
                sendMessage(fileName, buf, read, false);
                total += read;
                buf = new byte[8096];
            }
            logger.info("File '{}' of size {} transferred in {}ms", file.getName(), total, (System.currentTimeMillis() - start));

        } catch (Exception e) {
            throw new RuntimeException("Failed to transfer", e);
        } finally {
            sendMessage(fileName, null, 0, true);
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

    private void sendMessage(String fileName, byte[] buf, int length, boolean eof) {
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

package io.joshworks.eventry.server.cluster.partition;

import io.joshworks.fstore.core.io.FileUtils;
import io.joshworks.fstore.core.io.IOUtils;
import org.jgroups.Message;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionReceiver {

    private final File partitionRoot;
    private final Map<String, OutputStream> files = new ConcurrentHashMap<>();

    public PartitionReceiver(File partitionRoot) {
        this.partitionRoot = partitionRoot;
        if(Files.exists(partitionRoot.toPath())) {
            FileUtils.deleteRecursively(partitionRoot);
        }
        partitionRoot.mkdir();
    }

    public void receive(Message msg) {
        FileHeader header = msg.getHeader(FileHeader.HEADER_ID);
        if (header == null) {
            return;
        }
        String fileName = header.fileName();
        OutputStream out = files.get(fileName);
        try {
            if (out == null) {
                out = createOutputStream(fileName);
                files.put(fileName, out);
            }
            if (header.eof()) {
                IOUtils.closeQuietly(files.remove(fileName));
            } else {
                out.write(msg.getRawBuffer(), msg.getOffset(), msg.getLength());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write partition file");
        }
    }

    private OutputStream createOutputStream(String fileName) throws IOException {
        Path filePath = partitionRoot.toPath().resolve(Paths.get(fileName));
        Path fileFolder = filePath.getParent();
        if(!Files.exists(fileFolder)) {
            Files.createDirectory(fileFolder);
        }
        return new FileOutputStream(filePath.toFile());
    }

}

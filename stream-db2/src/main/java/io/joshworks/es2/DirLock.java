package io.joshworks.es2;

import io.joshworks.fstore.core.RuntimeIOException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DirLock implements Closeable {

    private static final String LOCK_FILE = ".lock";

    private final Path path;
    private final FileChannel lockChannel;
    private final FileLock fileLock;

    public DirLock(File root) {
        try {
            this.path = new File(root, LOCK_FILE).toPath();
            this.lockChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
            this.fileLock = lockChannel.tryLock();
            if (fileLock == null) {
                throw new IOException("Failed to acquire lock");
            }
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to acquire directory lock: ", e);
        }
    }

    @Override
    public void close() {
        try {
            fileLock.release();
            lockChannel.close();
            Files.deleteIfExists(path);
        } catch (Exception e) {
            System.err.println("Failed to release lock");
            e.printStackTrace();
        }
    }
}
package io.joshworks.fstore.core.util;

import io.joshworks.fstore.core.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class FileUtils {

    private static final Logger log = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Attempts to move source to target atomically and falls back to a non-atomic move if it fails.
     *
     * @throws IOException if both atomic and non-atomic moves fail
     */
    public static void tryMoveAtomically(Path source, Path target) {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException outer) {
            try {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
                log.debug("Non-atomic move of {} to {} succeeded after atomic move failed due to {}", source, target, outer.getMessage());
            } catch (IOException inner) {
                inner.addSuppressed(outer);
                throw new RuntimeIOException("Failed to move " + source + " to " + target, inner);
            }
        }
    }

    public static void tryCreate(File file) {
        boolean newFile = FileUtils.createIfNotExists(file);
        if (!newFile) {
            throw new RuntimeIOException("File already exist " + file.getAbsolutePath());
        }
    }

    /**
     * returns true if the file was created, false otherwise
     */
    public static boolean createIfNotExists(File file) {
        try {
            if (!Files.exists(file.toPath())) {
                Files.createFile(file.toPath());
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to create " + file, e);
        }
    }

    /**
     * returns true if the file was created, false otherwise
     */
    public static void createDir(File file) {
        try {
            if (!Files.exists(file.toPath())) {
                Files.createDirectory(file.toPath());
            }
            if (!file.isDirectory()) {
                throw new IllegalArgumentException("Not a directory: " + file.getAbsoluteFile());
            }
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to create " + file, e);
        }
    }

    /**
     * returns true if the file was deleted, false otherwise
     */
    public static boolean deleteIfExists(File file) {
        try {
            return Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to delete " + file, e);
        }
    }


}

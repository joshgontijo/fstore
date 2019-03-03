package io.joshworks.fstore.core.io;

import java.io.File;
import java.nio.file.Files;

public class FileUtils {

    private FileUtils() {
    }

    public static void deleteRecursively(File file) {
        try {
            if (file.isDirectory()) {
                String[] list = file.list();
                if (list != null) {
                    for (String f : list) {
                        File item = new File(file, f);
                        if (item.isDirectory()) {
                            deleteRecursively(item);
                        }
                        Files.deleteIfExists(item.toPath());
                    }
                }
            }
            Files.deleteIfExists(file.toPath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

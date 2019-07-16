package io.joshworks.fstore.testutils;

import io.joshworks.fstore.core.properties.AppProperties;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.TreeSet;
import java.util.UUID;

public class FileUtils {

    public static final String TEST_DIR = "test.dir";
    public static final String FSTORE_TEST = "fstore-test";
    public static final String TEST_FILES = "test-files";

    public static void main(String[] args) {

        TreeSet<Integer> set = new TreeSet<>();
        set.add(1);
        set.add(5);
        set.add(10);
        set.add(20);

        Integer floor = set.floor(1);
        System.out.println(floor);


//        ByteBuffer bb1 = ByteBuffer.allocate(4).put("aa".getBytes(StandardCharsets.UTF_8)).flip();
//        ByteBuffer bb2 = ByteBuffer.allocate(4).put("aaa".getBytes(StandardCharsets.UTF_8)).flip();

        ByteBuffer bb1 = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).flip();
        ByteBuffer bb2 = ByteBuffer.allocate(4).putInt(Integer.MIN_VALUE).flip();

        System.out.println(bb1.compareTo(bb2));

    }

    //terrible work around for waiting the mapped data to release file lock
    public static void tryDelete(File file) {
        int maxTries = 2;
        int counter = 0;
        while (counter++ < maxTries) {
            try {
                if (file.isDirectory()) {
                    String[] list = file.list();
                    if (list != null) {
                        for (String f : list) {
                            File item = new File(file, f);
                            if (item.isDirectory()) {
                                tryDelete(item);
                            }
                            System.out.println("Deleting " + item);
                            Files.deleteIfExists(item.toPath());
                        }
                    }
                }
                System.out.println("Deleting " + file);
                Files.deleteIfExists(file.toPath());
                break;
            } catch (Exception e) {
                System.err.println(":: FAILED TO DELETE FILE :: " + e.getMessage());
                e.printStackTrace();
                sleep(2000);
            }
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
    }


    private static File tempFolder() {
        try {
            return Files.createTempDirectory(FSTORE_TEST).toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File testFile() {
        return testFile(UUID.randomUUID().toString().substring(0, 8));
    }

    public static File testFile(String name) {
        return new File(testFolder(TEST_FILES), name);
    }

    public static File testFolder() {
        return testFolder(UUID.randomUUID().toString().substring(0, 8));
    }

    private static File testFolder(String name) {
        try {
            File testDir = AppProperties.create().get(TEST_DIR).map(File::new).orElse(tempFolder());
            File file = new File(testDir, name);
            Files.createDirectories(file.toPath());
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

//package io.joshworks.fstore.core.utils;
//
//import io.joshworks.fstore.core.properties.AppProperties;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.UUID;
//
//public class Utils {
//
//    //terrible work around for waiting the mapped pack to release file lock
//    public static void tryDelete(File file) {
//        int maxTries = 5;
//        int counter = 0;
//        while (counter++ < maxTries) {
//            try {
//                if (file.isDirectory()) {
//                    String[] list = file.list();
//                    if (list != null)
//                        for (String f : list) {
//                            Path path = new File(file, f).toPath();
//                            System.out.println("Deleting " + path);
//                            if (!Files.deleteIfExists(path)) {
//                                throw new RuntimeException("Failed to delete file");
//                            }
//                        }
//                }
//                if (file.exists() && !Files.deleteIfExists(file.toPath())) {
//                    throw new RuntimeException("Failed to delete file");
//                }
//                break;
//            } catch (Exception e) {
//                System.err.println(file.getPath() + " :: LOCK NOT RELEASED YET :: " + e.getMessage());
//                sleep(2000);
//            }
//        }
//    }
//
//    public static void removeFiles(File directory) throws IOException {
//        String[] files = directory.list();
//        if (files != null) {
//            for (String s : files) {
//                System.out.println("Deleting " + s);
//                File file = new File(directory, s);
//                if (file.isDirectory()) {
//                    removeFiles(file);
//                }
//                Files.delete(file.toPath());
//            }
//        }
//        Files.delete(directory.toPath());
//    }
//
//    public static void sleep(long millis) {
//        try {
//            Thread.sleep(millis);
//        } catch (InterruptedException e1) {
//            e1.printStackTrace();
//        }
//    }
//
//    public static File TEST_DIR = AppProperties.create().get("test.dir")
//            .map(File::new)
//            .orElse(tempFolder());
//
//    public static File tempFolder() {
//        try {
//            return Files.createTempDirectory("eventry").toFile();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static File testFile() {
//        return testFile(UUID.randomUUID().toString().substring(0, 8));
//    }
//
//    public static File testFile(String name) {
//        return new File(testFolder(), name);
//    }
//
//    public static File testFolder() {
//        return testFolder(UUID.randomUUID().toString().substring(0, 8));
//    }
//
//    public static File testFolder(String name) {
//        try {
//            File file = new File(TEST_DIR, name);
//            Files.createDirectories(file.toPath());
//            return file;
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//
////    private static void deleteDirectory(File dir) throws IOException {
////        Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<>() {
////
////            @Override
////            public FileVisitResult visitFile(Path file,
////                                             BasicFileAttributes attrs) throws IOException {
////
////                System.out.println("Deleting file: " + file);
////                Files.delete(file);
////                return CONTINUE;
////            }
////
////            @Override
////            public FileVisitResult postVisitDirectory(Path dir,
////                                                      IOException exc) throws IOException {
////
////                System.out.println("Deleting dir: " + dir);
////                if (exc == null) {
////                    Files.delete(dir);
////                    return CONTINUE;
////                } else {
////                    throw exc;
////                }
////            }
////
////        });
////    }
//}

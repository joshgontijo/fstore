package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Size;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) throws IOException {
        Path path = Path.of("./test");
        Files.deleteIfExists(path);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ);
            byte[] data = new byte[4096];
            Arrays.fill(data, (byte) 1);

            long written = 0;
            long size = Size.GB.of(5);
            ByteBuffer bb = ByteBuffer.wrap(data);
            while (written < size) {
                written += channel.write(bb);
                bb.clear();
            }

            channel.force(true);
            System.out.println("Reading");

            read(channel, size, 4096 * 4);
            read(channel, size, 4096 * 2);
            read(channel, size, 4096);
            read(channel, size, 1024);
            read(channel, size, 500);
            read(channel, size, 200);
            read(channel, size, 70);


        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void read(FileChannel channel, long size, int readSize) throws IOException {
        var bbr = ByteBuffer.allocate(readSize);
        long read = 0;
        long totalTime = 0;
        long hits = 0;
        while (read < size) {
            long s = System.nanoTime();
            read += channel.read(bbr, read);
            totalTime += System.nanoTime() - s;
            hits++;
            bbr.clear();
        }

        System.out.println("SIZE: " + readSize);
        System.out.println("TOTAL: " + totalTime / 1000);
        System.out.println("HITS: " + hits);
        System.out.println("AVG: " + (totalTime / hits));
        System.out.println("--------------------");
    }
}

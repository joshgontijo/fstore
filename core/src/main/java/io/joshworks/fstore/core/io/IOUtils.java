/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package io.joshworks.fstore.core.io;


import io.joshworks.fstore.core.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;


public final class IOUtils {


    private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {

    }

    public static void writeFully(FileChannel channel, ByteBuffer buffer, byte[] bytes) throws IOException {
        if (bytes.length == 0) {
            return;
        }

        int lastWrittenIdx = 0;
        do {
            int length = Math.min(buffer.remaining(), bytes.length - lastWrittenIdx);
            buffer.put(bytes, lastWrittenIdx, length);
            lastWrittenIdx += length;

            buffer.flip();
            writeFully(channel, buffer);
            buffer.clear();

        } while (lastWrittenIdx < bytes.length);
    }

    public static int writeFully(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        int written = 0;
        do {
            int write = channel.write(buffer);
            written = write > 0 ? written + write : written;
        } while (buffer.hasRemaining());
        return written;
    }

    public static int readFully(FileChannel from, ByteBuffer buffer) throws IOException {
        return readFully(from, 0, buffer);
    }

    public static int readFully(FileChannel from, long offset, ByteBuffer buffer) throws IOException {
        int bytesRead = 0;
        while (buffer.hasRemaining()) {
            bytesRead += from.read(buffer, offset + bytesRead);
            if (bytesRead == -1) {
                throw new RuntimeIOException("Data stream ended prematurely");
            }
        }
        return bytesRead;
    }

    public static RandomAccessFile randomAccessFile(File file) {
        return randomAccessFile(file, "rw");
    }

    public static RandomAccessFile randomAccessFile(File file, long size) {
        if (size < 0) {
            throw new IllegalArgumentException("Invalid file size: " + size);
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.setLength(size);
            return raf;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static RandomAccessFile randomAccessFile(File file, String mode) {
        try {
            return new RandomAccessFile(file, mode);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    public static void readFully(RandomAccessFile from, byte[] to) throws IOException {
        int totalRead = 0;
        int length = to.length;
        if (length <= 0) {
            throw new IllegalArgumentException("Destination buffer position must be greater than zero");
        }

        while (totalRead < length) {
            int bytesRead = from.read(to, totalRead, to.length - totalRead);
            if (bytesRead < 0) {
                throw new IOException("Data stream ended prematurely");
            }
            totalRead += bytesRead;
        }
    }


    public static void readFully(RandomAccessFile from, byte[] to, int offset, int length) throws IOException {
        int totalRead = 0;
        if (length <= 0) {
            throw new IllegalArgumentException("Destination buffer position must be greater than zero");
        }

        while (totalRead < length) {
            int bytesRead = from.read(to, offset + totalRead, to.length - totalRead);
            if (bytesRead < 0) {
                throw new IOException("Data stream ended prematurely");
            }
            totalRead += bytesRead;
        }
    }

    public static void flush(Flushable flushable) {
        try {
            if (flushable != null) {
                flushable.flush();
            }
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception e) {
            logger.error("Error while closing resource", e);
        }
    }

    public static void releaseLock(FileLock lock) {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } catch (IOException e) {
            throw RuntimeIOException.of(e);
        }
    }

    public static String toString(InputStream is) {
        try (Scanner scanner = new Scanner(is, StandardCharsets.UTF_8)) {
            return scanner.useDelimiter("\\A").next();
        }
    }
}

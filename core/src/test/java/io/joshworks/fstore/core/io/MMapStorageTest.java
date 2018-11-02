package io.joshworks.fstore.core.io;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MMapStorageTest extends StorageTest {

    @Override
    protected Storage store(File file, long size) {
        return StorageProvider.of(StorageMode.MMAP).create(file, size);
    }


    @Test
    public void buffer_grows_bigger_than_original_size() {
        long originalLength = storage.length();
        byte[] data = new byte[]{1};

        while (storage.position() < originalLength) {
            storage.write(ByteBuffer.wrap(data));
        }

        storage.write(ByteBuffer.wrap(data));

        assertTrue(storage.length() > originalLength);
    }
}
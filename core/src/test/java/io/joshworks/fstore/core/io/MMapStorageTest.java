package io.joshworks.fstore.core.io;

import java.io.File;

public class MMapStorageTest extends StorageTest {

    @Override
    protected Storage store(File file, long size) {
        return StorageProvider.of(Mode.MMAP).create(file, size);
    }

}
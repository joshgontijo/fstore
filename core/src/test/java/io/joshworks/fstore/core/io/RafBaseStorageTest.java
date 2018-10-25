package io.joshworks.fstore.core.io;

import java.io.File;

public class RafBaseStorageTest extends DiskStorageTest {

    @Override
    protected Storage store(File file, long size) {
        return StorageProvider.raf().create(file, size);
    }

}
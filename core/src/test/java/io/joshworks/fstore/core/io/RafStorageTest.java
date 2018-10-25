package io.joshworks.fstore.core.io;

import java.io.File;

public class RafStorageTest extends StorageTest {

    @Override
    protected Storage store(File file, long size) {
        return StorageProvider.of(Mode.RAF).create(file, size);
    }

}
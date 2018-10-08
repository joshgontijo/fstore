package io.joshworks.fstore.lsm.sstable;

import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;

import java.io.File;
import java.nio.ByteBuffer;

public class VLenStorage extends RafStorage {

    public VLenStorage(File target, long length, Mode mode) {
        super(target, length, mode);
    }

    @Override
    public int write(ByteBuffer data) {
        return super.write(data);
    }
}

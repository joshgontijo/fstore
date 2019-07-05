package io.joshworks.fstore.index;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.LocalGrowingBufferPool;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SparseIndexTest {

    private Storage storage;
    private DataStream stream;
    private BlockFactory blockFactory = VLenBlock.factory();

    @Before
    public void setUp() {
        storage = Storage.create(FileUtils.testFile(), StorageMode.RAF, 4096);
        stream = new DataStream(new LocalGrowingBufferPool(false), storage, 4096, 1, 4096);
    }

    @After
    public void cleanup() {
        storage.delete();
    }

    @Test
    public void name() {

        LogHeader header = LogHeader.read(storage);
        header.writeNew(storage, WriteMode.LOG_HEAD, 4096, 4096, false);

        FooterReader reader = new FooterReader(stream, header);
        SparseIndex<String> index = new SparseIndex<>(Serializers.VSTRING, 1024, reader, blockFactory);

        for (int i = 0; i < 10000; i++) {
            index.add(String.valueOf(i), i);
        }

        FooterWriter writer = new FooterWriter(stream);
        index.writeTo(writer);
        header.writeCompleted(storage, 0, 0, 0, writer.length(), 0);

        index = new SparseIndex<>(Serializers.VSTRING, 1024, reader, blockFactory);
        index.load();

        long pos = index.get("0");
        assertEquals(0, pos);
    }
}
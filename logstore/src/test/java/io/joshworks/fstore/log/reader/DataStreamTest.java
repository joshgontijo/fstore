package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.BufferPool1;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.RecordReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class DataStreamTest {

    private File file;
    private DataStream<Integer> dataStream;
    private Storage storage;

    @Before
    public void setUp() {
        file = Utils.testFile();
        storage = new RafStorage(file, 1024 * 1024, Mode.READ_WRITE);
        storage.position(Log.START);

    }

    @After
    public void tearDown() throws Exception {
        storage.close();
        Utils.tryDelete(file);
    }

    @Test
    public void write_returns_position() {
        DataStream<Integer> dataStream = new DataStream<>(Serializers.INTEGER, new BufferPool1(1024, 10, false));
        long write = dataStream.write(storage, 1);
        assertEquals(0, write);
    }

    @Test
    public void forward_reader_returns_all_data() {
        DataStream<Integer> dataStream = new DataStream<>(Serializers.INTEGER, new BufferPool1(1024, 10, false));
        dataStream.write(storage, 1);
        dataStream.write(storage, 2);
        dataStream.write(storage, 3);

        RecordReader<Integer> reader = dataStream.reader(storage, Log.START, Direction.FORWARD);
        assertEquals(Integer.valueOf(1), reader.readNext());
        assertEquals(Integer.valueOf(2), reader.readNext());
        assertEquals(Integer.valueOf(3), reader.readNext());
    }

    @Test
    public void backward_reader_returns_all_data() {
        DataStream<Integer> dataStream = new DataStream<>(Serializers.INTEGER, new BufferPool1(1024, 10, false));
        dataStream.write(storage, 1);
        dataStream.write(storage, 2);
        dataStream.write(storage, 3);

        RecordReader<Integer> reader = dataStream.reader(storage, storage.position(), Direction.BACKWARD);
        assertEquals(Integer.valueOf(3), reader.readNext());
        assertEquals(Integer.valueOf(2), reader.readNext());
        assertEquals(Integer.valueOf(1), reader.readNext());
    }


    @Test
    public void reader() {
    }
}
package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.StreamBlock;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StreamBlockDeserializer {

    public static List<TestEvent> deserialize(ByteBuffer block) {
        assert StreamBlock.isValid(block);

        int uncompressedSize = StreamBlock.uncompressedSize(block);
        byte codecId = StreamBlock.codec(block);

        ByteBuffer decompressed = Buffers.allocate(uncompressedSize, false);
        Codec codec = BlockCodec.from(codecId);

        int chunkSize = StreamBlock.sizeOf(block);
        ByteBuffer dataSlice = block.slice(block.position() + StreamBlock.HEADER_BYTES, chunkSize - StreamBlock.HEADER_BYTES);
        codec.decompress(dataSlice, decompressed);
        decompressed.flip();

        assert decompressed.remaining() == uncompressedSize;

        List<TestEvent> events = new ArrayList<>();
        while (decompressed.hasRemaining()) {
            int evLen = Event.sizeOf(decompressed);
            events.add(TestEvent.from(decompressed));
            Buffers.offsetPosition(decompressed, evLen);
        }
        return events;
    }

}

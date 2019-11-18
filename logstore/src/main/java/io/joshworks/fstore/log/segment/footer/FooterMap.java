package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordEntry;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.log.segment.block.Blocks;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.collection.EntrySerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class FooterMap {

    private final Serializer<Map.Entry<String, Long>> entrySerializer = new EntrySerializer<>(Serializers.VSTRING, Serializers.LONG);
    private final Map<String, Long> items = new ConcurrentHashMap<>();
    static final long NONE = -1;
    private final BlockFactory blockFactory = Block.vlenBlock();
    private final Serializer<Block> blockSerializer = new BlockSerializer(Codec.noCompression(), blockFactory);


    public long writeTo(DataStream stream, BufferPool bufferPool) {
        int maxEntrySize = stream.maxEntrySize() - RecordHeader.HEADER_OVERHEAD;
        Block block = blockFactory.create(Math.min(Memory.PAGE_SIZE * 2, maxEntrySize));


        long blockStartPos = stream.position();
        AtomicInteger blocks = new AtomicInteger();
        Blocks.writeAll(items.entrySet(), block, bufferPool, entrySerializer, b -> {
            stream.write(b, blockSerializer);
            blocks.incrementAndGet();
        });

        long mapIndexPos = stream.position();

        stream.write(blockStartPos, Serializers.LONG);
        stream.write(blocks.get(), Serializers.INTEGER);

        return mapIndexPos;

    }

    public void load(LogHeader header, DataStream stream) {
        if (!header.readOnly() || header.footerSize() == 0) {
            return;
        }

        long mapPosition = header.footerMapPosition();
        RecordEntry<Long> footerMapStarPos = stream.read(Direction.FORWARD, mapPosition, Serializers.LONG);
        RecordEntry<Integer> blockCount = stream.read(Direction.FORWARD, mapPosition + footerMapStarPos.recordSize(), Serializers.INTEGER);


        long pos = footerMapStarPos.entry();
        for (int i = 0; i < blockCount.entry(); i++) {
            RecordEntry<Block> footerMapBlock = stream.read(Direction.FORWARD, pos, blockSerializer);
            for (ByteBuffer entry : footerMapBlock.entry()) {
                Map.Entry<String, Long> kv = entrySerializer.fromBytes(entry);
                items.put(kv.getKey(), kv.getValue());
            }
            pos += footerMapBlock.recordSize();
        }
    }

    <T> void write(String name, DataStream stream, T entry, Serializer<T> serializer) {
        if (items.containsKey(name)) {
            throw new IllegalArgumentException("'" + name + "' already exist in the footer map");
        }
        long position = stream.write(entry, serializer);
        items.put(name, position);
    }

    List<Long> findAll(String prefix) {
        return items.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(prefix)).map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    long get(String name) {
        return items.getOrDefault(name, NONE);
    }

}

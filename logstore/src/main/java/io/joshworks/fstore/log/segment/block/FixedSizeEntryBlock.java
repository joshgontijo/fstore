package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FixedSizeEntryBlock<T> implements Block<T> {


    private final Serializer<T> serializer;
    private boolean readOnly;
    private final List<T> cached = new ArrayList<>();
    private final List<ByteBuffer> buffers = new ArrayList<>();
    private final int entrySize;
    private final int maxEntries;

    public FixedSizeEntryBlock(Serializer<T> serializer, int maxSize, int entrySize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        if (entrySize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        this.entrySize = entrySize;
        this.serializer = serializer;
        this.maxEntries = maxSize / entrySize;
    }

    protected FixedSizeEntryBlock(Serializer<T> serializer, Codec codec, ByteBuffer data) {
        this.serializer = serializer;
        this.readOnly = true;
        HeaderInfo header = this.unpack(codec, data);
        this.entrySize = header.entrySize;
        this.maxEntries = header.entrySize * header.entryCount;
    }

    @Override
    public boolean add(T data) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        ByteBuffer bb = serializer.toBytes(data);
        buffers.add(bb);
        return buffers.size() >= maxEntries;
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        readOnly = true;
        int entryCount = buffers.size();
        int totalSize = entryCount * entrySize;

        ByteBuffer withHeader = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + totalSize);
        withHeader.putInt(entryCount);
        withHeader.putInt(entrySize);
        for (ByteBuffer buffer : buffers) {
            if (buffer.remaining() == 0) {
                throw new IllegalStateException("Block is empty");
            }
            withHeader.put(buffer);
        }

        withHeader.flip();
        return codec.compress(withHeader);
    }

    private HeaderInfo unpack(Codec codec, ByteBuffer blockData) {
        ByteBuffer decompressed = codec.decompress(blockData);
        int entryCount = decompressed.getInt();
        int entriesSize = decompressed.getInt();
        for (int i = 0; i < entryCount; i++) {
            //copy, so avoid shared data on BufferRef
            //an off heap buffer could also be used
            byte[] copy = new byte[entriesSize];
            decompressed.get(copy);
            buffers.add(ByteBuffer.wrap(copy));
        }
        return new HeaderInfo(entryCount, entriesSize);
    }

    @Override
    public int entryCount() {
        return buffers.size();
    }

    @Override
    public List<T> entries() {
        return new ArrayList<>(cached);
    }

    @Override
    public T first() {
        if (cached.isEmpty()) {
            return null;
        }
        return cached.get(0);
    }

    @Override
    public T last() {
        if (cached.isEmpty()) {
            return null;
        }
        return get(cached.size() - 1);
    }

    @Override
    public boolean readOnly() {
        return readOnly;
    }

    @Override
    public boolean isEmpty() {
        return buffers.isEmpty();
    }

    @Override
    public List<Integer> entriesLength() {
        return buffers.stream().map(i -> entrySize).collect(Collectors.toList());
    }

    @Override
    public T get(int idx) {
        if (cached.isEmpty() || idx >= cached.size() || idx < 0) {
            return null;
        }
        return cached.get(idx);
    }

    private static class HeaderInfo {
        private final int entryCount;
        private final int entrySize;

        private HeaderInfo(int entryCount, int entrySize) {
            this.entryCount = entryCount;
            this.entrySize = entrySize;
        }
    }

    public static <T> BlockFactory<T> factory(int entrySize) {
        return new FixedSizeBlockFactory<>(entrySize);
    }

    private static class FixedSizeBlockFactory<T> implements BlockFactory<T> {

        private final int entrySize;

        private FixedSizeBlockFactory(int entrySize) {
            this.entrySize = entrySize;
        }

        @Override
        public Block<T> create(Serializer<T> serializer, int maxBlockSize) {
            return new FixedSizeEntryBlock<>(serializer, maxBlockSize, entrySize);
        }

        @Override
        public Block<T> load(Serializer<T> serializer, Codec codec, ByteBuffer data) {
            return new FixedSizeEntryBlock<>(serializer, codec, data);
        }
    }

}

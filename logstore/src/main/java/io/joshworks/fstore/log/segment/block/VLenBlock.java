package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class VLenBlock<T> implements Block<T> {

    private final Serializer<T> serializer;
    private final int maxSize;

    private boolean readOnly;
    private int totalSize;
    private final List<Integer> lengths = new ArrayList<>();
    private final List<T> cached = new ArrayList<>();
    private final List<ByteBuffer> buffers = new ArrayList<>();

    public VLenBlock(Serializer<T> serializer, int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        this.serializer = serializer;
        this.maxSize = maxSize;
    }

    protected VLenBlock(Serializer<T> serializer, Codec codec, ByteBuffer data) {
        this.serializer = serializer;
        this.readOnly = true;
        this.maxSize = data.limit();
        this.unpack(codec, data);
    }

    @Override
    public boolean add(T data) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        ByteBuffer bb = serializer.toBytes(data);
        lengths.add(bb.limit());
        totalSize += bb.limit();
        buffers.add(bb);
        return totalSize >= maxSize;
    }

    static <T> T readEntry(ByteBuffer data, Serializer<T> serializer, int length) {
        if (data.remaining() == 0 || data.remaining() < length) {
            return null;
        }
        int original = data.limit();
        data.limit(data.position() + length);
        T entry = serializer.fromBytes(data);
        data.limit(original);
        return entry;
    }

//    ByteBuffer readOnlyBuffer() {
//        ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
//        //if not readonly, this block is being written to, flip it
//        return readOnly ? readOnlyBuffer : readOnlyBuffer.flip();
//    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        readOnly = true;
        int entryCount = entryCount();

        ByteBuffer withHeader = ByteBuffer.allocate(totalSize + Integer.BYTES + (Integer.BYTES * entryCount));
        withHeader.putInt(entryCount);
        for (int i = 0; i < entryCount; i++) {
            withHeader.putInt(lengths.get(i));
        }
        for (ByteBuffer buffer : buffers) {
            if (buffer.remaining() == 0) {
                throw new IllegalStateException("Block is empty");
            }
            withHeader.put(buffer);
        }

        withHeader.flip();

        return codec.compress(withHeader);
    }

    private void unpack(Codec codec, ByteBuffer blockData) {
        ByteBuffer decompressed = codec.decompress(blockData);
        int entryCount = decompressed.getInt();
        for (int i = 0; i < entryCount; i++) {
            lengths.add(decompressed.getInt());
        }

        blockData.mark();
        for (Integer length : lengths) {
            //safe to reuse ByteBuffer, since the DirectBuffer is not allowed when using BlockSegment
            int dataEnd = decompressed.position() + length;
            decompressed.limit(dataEnd);

            T item = serializer.fromBytes(decompressed);
            cached.add(item);
            decompressed.position(dataEnd);
        }
    }

    @Override
    public int entryCount() {
        return lengths.size();
    }

    @Override
    public List<T> entries() {
        return new ArrayList<>(cached);
//        if (!cached.isEmpty()) {
//            return new ArrayList<>(cached);
//        }
//        ByteBuffer readBuffer = readOnly ? buffer : buffer.asReadOnlyBuffer();
//        for (Integer length : lengths) {
//            T entry = readEntry(readBuffer, serializer, length);
//            cached.add(entry);
//        }
//        return new ArrayList<>(cached);
    }

    @Override
    public T first() {
        if (lengths.isEmpty()) {
            return null;
        }
        return get(0);
    }

    @Override
    public T last() {
        if (lengths.isEmpty()) {
            return null;
        }
        return get(lengths.size() - 1);
    }

    @Override
    public T get(int idx) {
        if (cached.isEmpty() || idx >= cached.size() || idx < 0) {
            return null;
        }
        return cached.get(idx);
    }

    @Override
    public boolean readOnly() {
        return readOnly;
    }

    @Override
    public boolean isEmpty() {
        return lengths.isEmpty();
    }

    @Override
    public List<Integer> entriesLength() {
        return lengths;
    }

    public static <T> BlockFactory<T> factory() {
        return new VLenBlockFactory<>();
    }

    private static class VLenBlockFactory<T> implements BlockFactory<T> {

        @Override
        public VLenBlock<T> create(Serializer<T> serializer, int maxBlockSize) {
            return new VLenBlock<>(serializer, maxBlockSize);
        }

        @Override
        public Block<T> load(Serializer<T> serializer, Codec codec, ByteBuffer data) {
            return new VLenBlock<>(serializer, codec, data);
        }
    }

}

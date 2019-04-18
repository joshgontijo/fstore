package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseBlock implements Block {

    private final int maxSize;
    protected boolean readOnly;

    private int totalSize;
    protected final List<ByteBuffer> buffers = new ArrayList<>();

    //returns the uncompressed size
    public BaseBlock(int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        this.maxSize = maxSize;
    }

    protected BaseBlock(Codec codec, ByteBuffer data) {
        this.readOnly = true;
        this.maxSize = this.unpack(codec, data);
    }

    //returns true if added, false otherwise
    @Override
    public boolean add(ByteBuffer data) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        int entrySize = data.limit();
        if (entrySize > maxSize) {
            throw new IllegalArgumentException("Record size (" + entrySize + ") cannot be greater than blockSize (" + maxSize + ")");
        }
        if (totalSize + data.limit() > maxSize) {
            return false;
        }
        if (!data.hasRemaining()) {
            throw new IllegalArgumentException("Empty buffer is not allowed");
        }
        totalSize += data.limit();
        buffers.add(data.asReadOnlyBuffer());
        return true;
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        readOnly = true;
        int entryCount = entryCount();

        ByteBuffer withHeader = ByteBuffer.allocate(Integer.BYTES + (Integer.BYTES * entryCount) + totalSize);
        withHeader.putInt(entryCount);

        for (int i = 0; i < entryCount; i++) {
            withHeader.putInt(buffers.get(i).remaining());
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

    protected int unpack(Codec codec, ByteBuffer blockData) {
        ByteBuffer decompressed = codec.decompress(blockData);
        int entryCount = decompressed.getInt();
        int[] lengths = new int[entryCount];
        for (int i = 0; i < entryCount; i++) {
            lengths[i] = decompressed.getInt();
        }

        int decompressedLength = 0;
        for (int length : lengths) {
            //safe to reuse ByteBuffer, since the DirectBuffer is not allowed when using BlockSegment
            int dataEnd = decompressed.position() + length;
            decompressed.limit(dataEnd);
            ByteBuffer bb = decompressed.slice().asReadOnlyBuffer();
            decompressedLength += bb.limit();
            buffers.add(bb);
            decompressed.position(dataEnd);
        }
        return decompressedLength;
    }


    @Override
    public int entryCount() {
        return buffers.size();
    }

    @Override
    public List<ByteBuffer> entries() {
        return buffers.stream().map(ByteBuffer::asReadOnlyBuffer).collect(Collectors.toList());
    }

    @Override
    public ByteBuffer first() {
        if (buffers.isEmpty()) {
            return null;
        }
        return get(0);
    }

    @Override
    public ByteBuffer last() {
        if (buffers.isEmpty()) {
            return null;
        }
        return get(buffers.size() - 1);
    }

    @Override
    public ByteBuffer get(int idx) {
        if (buffers.isEmpty() || idx >= buffers.size() || idx < 0) {
            return null;
        }
        return buffers.get(idx).asReadOnlyBuffer();
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
        return buffers.stream().mapToInt(ByteBuffer::limit).boxed().collect(Collectors.toList());
    }

    @Override
    public int uncompressedSize() {
        return totalSize;
    }

    @Override
    public <T> List<T> deserialize(Serializer<T> serializer) {
        List<T> items = new ArrayList<>();
        for (ByteBuffer buffer : buffers) {
            T item = serializer.fromBytes(buffer.asReadOnlyBuffer());
            items.add(item);
        }
        return items;
    }

}

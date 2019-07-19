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

    private int decompressedLength;
    private int compressedLength;
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
        this.compressedLength = data.remaining();
        this.decompressedLength = this.unpack(codec, data);
        this.maxSize = decompressedLength;
    }

    //returns true if added, false otherwise
    @Override
    public boolean add(ByteBuffer data) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        validateEntry(data);
        if (decompressedLength + data.limit() > maxSize) {
            return false;
        }

        decompressedLength += data.limit();
        buffers.add(data);
        return true;
    }

    private void validateEntry(ByteBuffer data) {
        int entrySize = data.remaining();
        if (entrySize > maxSize) {
            throw new IllegalArgumentException("Record size (" + entrySize + ") cannot be greater than blockSize (" + maxSize + ")");
        }
        if (entrySize == 0) {
            throw new IllegalArgumentException("Empty buffer is not allowed");
        }
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        readOnly = true;
        int entryCount = entryCount();

        ByteBuffer withHeader = ByteBuffer.allocate(Integer.BYTES + (Integer.BYTES * entryCount) + decompressedLength);
        withHeader.putInt(entryCount);

        for (ByteBuffer buffer : buffers) {
            withHeader.putInt(buffer.remaining());
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

        int decompressedLen = 0;
        for (int length : lengths) {
            if (length > decompressed.capacity() || length < 0) {
                throw new IllegalStateException("Bad block entry length");
            }
            //safe to reuse ByteBuffer, since the DirectBuffer is not allowed when using BlockSegment
            int dataEnd = decompressed.position() + length;
            decompressed.limit(dataEnd);
            ByteBuffer slice = decompressed.slice();
            decompressedLen += slice.limit();
            buffers.add(slice);
            decompressed.position(dataEnd);
        }
        return decompressedLen;
    }


    @Override
    public int entryCount() {
        return buffers.size();
    }

    @Override
    public List<ByteBuffer> entries() {
        List<ByteBuffer> copies = new ArrayList<>(buffers.size());
        for (ByteBuffer buffer : buffers) {
            copies.add(asReadOnly(buffer));
        }
        return copies;
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
        return asReadOnly(buffers.get(idx));
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
        return decompressedLength;
    }

    @Override
    public int compressedSize() {
        if (!readOnly) {
            throw new IllegalStateException("Not read only");
        }
        return compressedLength;
    }

    @Override
    public <T> List<T> deserialize(Serializer<T> serializer) {
        List<T> items = new ArrayList<>();
        for (ByteBuffer buffer : buffers) {
            T item = serializer.fromBytes(asReadOnly(buffer));
            items.add(item);
        }
        return items;
    }

    private ByteBuffer asReadOnly(ByteBuffer bb) {
        return bb.asReadOnlyBuffer().clear();
    }

}

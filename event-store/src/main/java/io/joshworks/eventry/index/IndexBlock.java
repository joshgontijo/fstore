//package io.joshworks.eventry.index;
//
//import io.joshworks.fstore.core.Codec;
//import io.joshworks.fstore.core.Serializer;
//import io.joshworks.fstore.core.io.buffers.BufferPool;
//import io.joshworks.fstore.log.segment.block.Block;
//import io.joshworks.fstore.log.segment.block.BlockFactory;
//import io.joshworks.fstore.log.segment.block.FixedSizeEntryBlock;
//import io.joshworks.fstore.lsmtree.sstable.Entry;
//import io.joshworks.fstore.lsmtree.sstable.EntrySerializer;
//import io.joshworks.fstore.serializer.Serializers;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.List;
//
//public class IndexBlock extends FixedSizeEntryBlock {
//
//    private static final Serializer<Entry<IndexKey, Long>> serializer = new EntrySerializer<>(new IndexKeySerializer(), Serializers.LONG);
//    private static final int ENTRY_SIZE = IndexKey.BYTES + Long.BYTES;
//    private final boolean direct;
//
//    public IndexBlock(int maxSize, boolean direct) {
//        super(maxSize, direct, ENTRY_SIZE);
//        this.direct = direct;
//    }
//
//    protected IndexBlock(Codec codec, ByteBuffer data, boolean direct) {
//        super(codec, data, direct);
//        this.direct = direct;
//    }
//
//    @Override
//    protected void writeBlockContent(Codec codec, ByteBuffer dst) {
//
//        Entry<IndexKey, Long> last = null;
//        List<Integer> versions = new ArrayList<>();
//        List<Long> positions = new ArrayList<>();
//
//        if (data.remaining() % ENTRY_SIZE != 0) {
//            throw new IllegalStateException("Bad index block: Invalid block length");
//        }
//
//        int maxBufferSize = maxBufferSize(entryCount());
//        ByteBuffer tmp = BufferPool.createBuffer(maxBufferSize, direct);
//        while (data.hasRemaining()) {
//            Entry<IndexKey, Long> entry = serializer.fromBytes(data);
//            if (last == null) {
//                last = entry;
//            }
//            if (last.key.stream != entry.key.stream) {
//                writeToBuffer(tmp, last.key.stream, versions, positions);
//                versions = new ArrayList<>();
//                positions = new ArrayList<>();
//            }
//
//            versions.add(entry.key.version);
//            positions.add(entry.value);
//            last = entry;
//        }
//
//        if (last != null && !versions.isEmpty()) {
//            writeToBuffer(tmp, last.key.stream, versions, positions);
//        }
//
//        tmp.flip();
////        dst.put(tmp);
//        codec.compress(tmp, dst);
//    }
//
//    private int maxBufferSize(int entryCount) {
//        return entryCount * (ENTRY_SIZE + Integer.BYTES);
//    }
//
//    @Override
//    protected ByteBuffer unpack(Codec codec, ByteBuffer compressed, boolean direct) {
//        //header
//        int entryCount = compressed.getInt(); //parent
//        int uncompressedSize = compressed.getInt(); //parent
//        int entrySize = compressed.getInt();
//
//        if (entrySize * entryCount != uncompressedSize) {
//            throw new IllegalStateException("Expected block to have uncompressed size of " + (entrySize * entryCount) + " bytes, got " + (uncompressedSize));
//        }
//
//        //second level compression
//        ByteBuffer uncompressed = createBuffer(uncompressedSize, direct);
//        codec.decompress(compressed, uncompressed);
//        uncompressed.flip();
//
//        //first level compression
////        ByteBuffer uncompressed = compressed;
//        int bufferSize = maxBufferSize(entryCount);
//        ByteBuffer data = createBuffer(bufferSize, direct);
//
//        while (uncompressed.hasRemaining()) {
//            long stream = uncompressed.getLong();
//            int numVersions = uncompressed.getInt();
//            for (int i = 0; i < numVersions; i++) {
//                lengths.add(entrySize);
//                positions.add(data.position());
//
//                int version = uncompressed.getInt();
//                long position = uncompressed.getLong();
//                Entry<IndexKey, Long> ie = Entry.of(new IndexKey(stream, version), position);
//                serializer.writeTo(ie, data);
//            }
//        }
//        return data;
//    }
//
//    private void writeToBuffer(ByteBuffer buffer, long stream, List<Integer> versions, List<Long> positions) {
//        buffer.putLong(stream);
//        buffer.putInt(versions.size());
//        for (int i = 0; i < versions.size(); i++) {
//            buffer.putInt(versions.get(i));
//            buffer.putLong(positions.get(i));
//        }
//    }
//
//    static BlockFactory factory(boolean direct) {
//        return new IndexBlockFactory(direct);
//    }
//
//    private static class IndexBlockFactory implements BlockFactory {
//
//        private final boolean direct;
//
//        private IndexBlockFactory(boolean direct) {
//            this.direct = direct;
//        }
//
//        @Override
//        public Block create(int blockSize) {
//            return new IndexBlock(blockSize, direct);
//        }
//
//        @Override
//        public Block load(Codec codec, ByteBuffer data) {
//            return new IndexBlock(codec, data, direct);
//        }
//    }
//}

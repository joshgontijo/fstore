//package io.joshworks.eventry.index;
//
//import io.joshworks.fstore.core.Codec;
//import io.joshworks.fstore.core.Serializer;
//import io.joshworks.fstore.lsmtree.sstable.Entry;
//import io.joshworks.fstore.lsmtree.sstable.EntrySerializer;
//import io.joshworks.fstore.serializer.Serializers;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * Format:
// * [UNCOMPRESSED_SIZE][ENTRY_COUNT]
// * |streamHash entryCount | version1 pos1 | version2 pos2 | versionN posN |
// * <p>
// * Where:
// * streamHash - 64bits
// * entryCount - 32bits
// * version - 32bits
// * pos - 64bits
// * <p>
// * Definitions:
// * streamHash: any real number
// * entryCount: greater than zero
// * pos: greater or equal zero for actual positions. Long.MIN_VALUE for NULL
// * <p>
// * version: positive greater or equal zero for insertions, negative value representing the truncated before version
// * of the it's absolute value, example... -10 represents truncated versions from version 0 until 10.
// * So only version 11 onwards will be available.
// * <p>
// * Deleting a stream means the truncated version will be Integer.MIN_VALUE
// * <p>
// * This Codec only works with {@link io.joshworks.fstore.log.segment.block.FixedSizeEntryBlock}
// */
//public class IndexCodec implements Codec {
//
//    private final Serializer<Entry<IndexKey, Long>> serializer = new EntrySerializer<>(new IndexKeySerializer(), Serializers.LONG);
//
//    private static final int ENTTRY_SIZE = IndexKey.BYTES + Long.BYTES;
//
//    @Override
//    public void compress(ByteBuffer src, ByteBuffer dst) {
//
//        Entry<IndexKey, Long> last = null;
//        List<Integer> versions = new ArrayList<>();
//        List<Long> positions = new ArrayList<>();
//
//        if (src.remaining() % ENTTRY_SIZE != 0) {
//            throw new IllegalStateException("Bad index block: Invalid block length");
//        }
//
//        while (src.hasRemaining()) {
//            Entry<IndexKey, Long> entry = serializer.fromBytes(src);
//            if (last == null) {
//                last = entry;
//            }
//            if (last.key.stream != entry.key.stream) {
//                writeToBuffer(dst, last.key.stream, versions, positions);
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
//            writeToBuffer(dst, last.key.stream, versions, positions);
//        }
//    }
//
//    @Override
//    public void decompress(ByteBuffer src, ByteBuffer dst) {
//        while (src.hasRemaining()) {
//            long stream = src.getLong();
//            int numVersions = src.getInt();
//            for (int i = 0; i < numVersions; i++) {
//                int version = src.getInt();
//                long position = src.getLong();
////                Long p = position == NULL ? null : position;
//                Entry<IndexKey, Long> ie = Entry.of(new IndexKey(stream, version), position);
//                serializer.writeTo(ie, dst);
//            }
//        }
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
//}

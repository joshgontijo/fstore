//package io.joshworks.ilog.lsm;
//
//import io.joshworks.fstore.core.io.buffers.Buffers;
//
//import java.nio.ByteBuffer;
//import java.util.Queue;
//import java.util.concurrent.ArrayBlockingQueue;
//
//public class BufferCache {
//
//    private final ByteBuffer cache;
//    private final int regionSize;
//    private final Queue<Entry> timestamps;
//
//    public BufferCache(int regionSize, int maxSize, boolean direct) {
//        int actualSize = (maxSize / regionSize) * regionSize;
//        this.cache = Buffers.allocate(actualSize, direct);
//        this.regionSize = regionSize;
//        this.timestamps = new ArrayBlockingQueue<>(actualSize / regionSize);
//    }
//
//    public void add(ByteBuffer key, ByteBuffer data) {
//
//    }
//
//    private static final class Entry {
//        private long timestamp;
//        private int index;
//    }
//
//}

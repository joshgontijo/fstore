//package io.joshworks.fstore.core.io.buffers;
//
//
//
//import java.io.Closeable;
//import java.lang.ref.Cleaner;
//import java.nio.ByteBuffer;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.Queue;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentLinkedDeque;
//
//import static io.joshworks.fstore.core.io.MemStorage.MAX_BUFFER_SIZE;
//
///**
// * Single dynamic sized buffer, cached per thread.
// * Throws IllegalState exception if the buffer is tried to be allocated without releasing it first
// * A single thread may only allocate a single buffer at the time,
// * allocating more than once without freeing the previous buffer will corrupt the buffer contents
// */
//public class BufferPool2 implements Closeable {
//
//    private final ThreadLocal<LocalAllocations> cache = ThreadLocal.withInitial(LocalAllocations::new);
//    private final Queue<BufferRef> pool = new ConcurrentLinkedDeque<>();
//    private final Set<Region> regions = Collections.newSetFromMap(new ConcurrentHashMap<>());
//
//    private final int bufferPerRegion;
//    private final int bufferSize;
//    private final boolean direct;
//
//    private static final Cleaner cleaner = Cleaner.create();
//
//    public BufferPool2(int bufferSize, int bufferPerRegion, boolean direct) {
//        this.bufferSize = bufferSize;
//        this.bufferPerRegion = bufferPerRegion;
//        this.direct = direct;
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//
//        BufferPool2 bufferPool2 = new BufferPool2(1024, 3, false);
//        ByteBuffer b1 = bufferPool2.allocate().position(1);
//        ByteBuffer b2 = bufferPool2.allocate().position(2);
//        ByteBuffer b3 = bufferPool2.allocate().position(3);
//        ByteBuffer b4 = bufferPool2.allocate().position(4);
//
////        b1 = null;
////        b2 = null;
////        b3 = null;
////        b4 = null;
//
//        System.gc();
//
//        Thread.sleep(2000);
//
//        b1.putInt(1);
//
//    }
//
//    private Region allocateRegion(int size, boolean direct) {
//        if (size >= MAX_BUFFER_SIZE) {
//            throw new IllegalArgumentException("Buffer too large: Max allowed size is: " + MAX_BUFFER_SIZE);
//        }
//        ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
//        return new Region(buffer);
//    }
//
//    //allocate current buffer with its total capacity
//    public ByteBuffer allocate() {
//        BufferRef ref = pool.poll();
//        if (ref == null) {
//            synchronized (pool) {
//                ref = pool.poll();
//                if (ref != null) {
//                    return ref.allocate();
//                }
//                Region region = allocateRegion(bufferSize * bufferPerRegion, direct);
//                regions.add(region);
//            }
//        }
//        ref = pool.poll();
//        if (ref == null) {
//            throw new IllegalStateException("Could not allocate buffer");
//        }
//        return ref.allocate();
//    }
//
//    public boolean direct() {
//        return direct;
//    }
//
//    public int capacity() {
//        return bufferSize;
//    }
//
//    public void free() {
//        cache.get().free();
//    }
//
//    @Override
//    public void close() {
//        cache.get().free();
//        regions.clear();
//        pool.clear();
//    }
//
//    private class Region {
//
//        private final ByteBuffer buffer;
//        private Set<ByteBuffer> slices = new HashSet<>();
//
//        private Region(ByteBuffer buffer) {
//            this.buffer = buffer;
//            for (int i = 0; i < bufferPerRegion; i++) {
//                int start = i * bufferSize;
//                int end = start + bufferSize;
//                ByteBuffer slice = this.buffer.duplicate().position(start).limit(end).slice();
//                slices.add(slice);
//                BufferRef bufferRef = new BufferRef(slice);
//
//                //FIXME
//                cleaner.register(bufferRef, () -> {
//                    //Repool
//                    System.out.println(slice);
//                    slices.remove(slice);
//                    pool.add(new BufferRef(slice));
//                });
//
//                pool.add(bufferRef);
//            }
//        }
//    }
//
//
//    private class BufferRef  {
//        private final ByteBuffer buffer;
//
//        private BufferRef(ByteBuffer buffer) {
//            this.buffer = buffer;
//        }
//
//        void free() {
//            System.out.println(">>>>>>> CLEANING <<<<<<<<");
//            cache.get().refs.remove(this);
//            pool.add(this);
//            buffer.clear();
//        }
//
//        ByteBuffer allocate() {
////            cache.get().refs.add(this);
//            return buffer.clear();
//        }
//    }
//
//    class LocalAllocations {
//        Set<BufferRef> refs = new HashSet<>();
//
//        private void free() {
//            for (BufferRef ref : refs) {
//                ref.free();
//                pool.add(ref);
//            }
//        }
//    }
//
//}
//package io.joshworks.fstore.log.reader;
//
//import io.joshworks.fstore.core.io.DataStream;
//import io.joshworks.fstore.core.io.Storage;
//
//import java.nio.ByteBuffer;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//import java.util.function.Supplier;
//
//public class PooledDataReader implements DataStream {
//
//    private final BlockingQueue<DataStream> readers;
//    private static final int DEFAULT_READER_SIZE = 1000;
//
//    public PooledDataReader(Supplier<DataStream> readerSupplier) {
//        this(DEFAULT_READER_SIZE, readerSupplier);
//    }
//
//    public PooledDataReader(int maxReaders, Supplier<DataStream> readerSupplier) {
//        this.readers = new ArrayBlockingQueue<>(maxReaders);
//        for (int i = 0; i < maxReaders; i++) {
//            this.readers.add(readerSupplier.get());
//        }
//    }
//
//    @Override
//    public ByteBuffer readForward(Storage storage, long position) {
//        return read(storage, position);
//    }
//
//    private ByteBuffer read(Storage storage, long position) {
//        DataStream reader = null;
//        try {
//            reader = readers.take();
//            return reader.readForward(storage, position);
//        } catch (InterruptedException e) {
//            throw new RuntimeException("Could not acquire reader", e);
//        } finally {
//            if (reader != null)
//                readers.offer(reader);
//        }
//    }
//
//    @Override
//    public ByteBuffer readBackward(Storage storage, long position) {
//        return read(storage, position);
//    }
//
//    @Override
//    public ByteBuffer getBuffer() {
//        throw new UnsupportedOperationException();
//    }
//}

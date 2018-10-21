//package io.joshworks.fstore.core.io;
//
//import io.joshworks.fstore.core.utils.Utils;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
//import java.util.Arrays;
//
//import static org.junit.Assert.assertArrayEquals;
//import static org.junit.Assert.assertEquals;
//
//@Ignore
//public class MMapStorageTest extends DiskStorageTest {
//
//    private File mmapFile;
//
//    @Before
//    public void setUpMMapFile() {
//        mmapFile = Utils.testFile();
//    }
//
//    @After
//    public void tearDown() {
//        Utils.tryDelete(mmapFile);
//    }
//
//    @Override
//    protected Storage store(File file, long size) {
//        return create(file, size);
//    }
//
//    private MMapStorage create(File file, long fileSize) {
//        return (MMapStorage) StorageProvider.mmap().create(file, fileSize);
//    }
//
//
//    @Test
//    public void multiple_buffers() throws IOException {
//        long fileSize = 1024;
//        int bufferSize = 512;
//        try(Storage storage = create(mmapFile, fileSize)) {
//
//            int dataSize = bufferSize / 2;
//            //first
//            storage.write(dummyData(dataSize));
//            assertEquals(dataSize, storage.position());
//
//            storage.write(dummyData(dataSize));
//            assertEquals(dataSize * 2, storage.position());
//
//            //second
//            storage.write(dummyData(dataSize));
//            assertEquals(dataSize * 3, storage.position());
//
//            storage.write(dummyData(dataSize));
//            assertEquals(dataSize * 4, storage.position());
//
//            assertEquals(fileSize, storage.position());
//        }
//    }
//
//    @Test
//    public void data_is_never_split() {
//        int entrySize = 4096;
//        long fileSize = bufferSize * 2;
//
//        try (var storage = create(mmapFile, fileSize)) {
//
//            var bb = ofSize(entrySize);
//            storage.write(bb);
//
//            var bb2 = ofSize(entrySize + 1);
//            storage.write(bb2);
//
//            assertEquals(2, storage.buffers.length);
//        }
//    }
//
//    @Test
//    public void correctly_write_data_bigger_than_buffer() {
//        int entrySize = 4096;
//        long fileSize = entrySize * 2;
//        try (var storage = create(mmapFile, fileSize)) {
//            int dataLen = bufferSize + 1;
//            var bb = ofSize(dataLen);
//            int written = storage.write(bb);
//
//            assertEquals(dataLen, written);
//
//            var readBuffer = ByteBuffer.allocate(dataLen);
//            int read = storage.read(0, readBuffer);
//            readBuffer.flip();
//
//            assertEquals(dataLen, read);
//            assertEquals(dataLen, readBuffer.remaining());
//
//        }
//    }
//
//    @Test
//    public void read_returns_correct_data_with_multiple_buffers() {
//        int entrySize = 4096;
//        int bufferSize = entrySize * 2;
//        long fileSize = (long) bufferSize * 2;
//        try (var storage = create(mmapFile, fileSize, bufferSize)) {
//
//            for (int i = 0; i < 10; i++) {
//                var bb = ofSize(entrySize);
//                storage.write(bb);
//            }
//
//            var expected = ofSize(entrySize);
//            for (int i = 0; i < 10; i++) {
//                var read = ByteBuffer.allocate(entrySize);
//                storage.read(i * entrySize, read);
//
//                assertArrayEquals(expected.array(), read.array());
//            }
//
//        }
//    }
//
//    @Test
//    public void shrink_resize_the_file_rounding_to_the_buffer_size() {
//        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
//        storage.write(bb);
//
//        long pos = storage.position();
//        storage.truncate(pos);
//
//        assertEquals(BUFFER_SIZE, storage.length());
//    }
//
//    private ByteBuffer ofSize(int size) {
//        ByteBuffer bb = ByteBuffer.allocate(size);
//        for (int i = 0; i < size; i++) {
//            bb.put((byte) 1);
//        }
//        bb.flip();
//        return bb;
//    }
//
//
//    private static ByteBuffer dummyData(int size) {
//        byte[] data = new byte[size];
//        Arrays.fill(data, (byte) 1);
//        return ByteBuffer.wrap(data);
//    }
//
//}
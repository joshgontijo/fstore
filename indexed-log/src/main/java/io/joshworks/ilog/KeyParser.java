package io.joshworks.ilog;

import java.nio.ByteBuffer;

public interface KeyParser<T> {

    void writeTo(T data, ByteBuffer dst);

    T readFrom(BufferReader src);

    T readFrom(ByteBuffer src);

    int keySize();

    KeyParser<Integer> INT = new KeyParser<>() {
        @Override
        public void writeTo(Integer data, ByteBuffer dst) {
            dst.putInt(data);
        }

        @Override
        public Integer readFrom(BufferReader src) {
            return src.getInt();
        }

        @Override
        public Integer readFrom(ByteBuffer src) {
            return src.getInt();
        }

        @Override
        public int keySize() {
            return Integer.BYTES;
        }
    };

    KeyParser<Long> LONG = new KeyParser<>() {
        @Override
        public void writeTo(Long data, ByteBuffer dst) {
            dst.putLong(data);
        }

        @Override
        public Long readFrom(BufferReader src) {
            return src.getLong();
        }

        @Override
        public Long readFrom(ByteBuffer src) {
            return src.getLong();
        }

        @Override
        public int keySize() {
            return Long.BYTES;
        }
    };

}

package io.joshworks.es2.sink;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.ByteArrayOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public interface Sink extends WritableByteChannel, Flushable {

    @Override
    int write(ByteBuffer src);

    @Override
    boolean isOpen();

    @Override
    void close();

    @Override
    void flush();


    class Memory implements Sink {
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        @Override
        public int write(ByteBuffer src) {
            try {
                byte[] data = Buffers.copyArray(src);
                baos.write(data);
                return data.length;
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
            //do nothing
        }

        @Override
        public void flush() {
            //do nothing
        }

        public byte[] data() {
            return baos.toByteArray();
        }
    }

}

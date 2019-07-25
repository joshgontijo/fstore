package io.joshworks.fstore.codec.std;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class DeflaterCodec implements Codec {

    @Override
    public void compress(ByteBuffer src, ByteBuffer dst) {
        try {
            Deflater deflater = new Deflater();
            deflater.setInput(src);
            deflater.finish();
            deflater.deflate(dst);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) {
        try {
            Inflater inflater = new Inflater();
            inflater.setInput(src);
            inflater.inflate(dst);
            inflater.end();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

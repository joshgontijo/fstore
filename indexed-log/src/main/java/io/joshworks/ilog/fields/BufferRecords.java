package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

public class BufferRecords {

    public static int sizeOf(ByteBuffer data, Field[] fields) {
        int size = 0;
        for (Field field : fields) {
            size += field.len(data);
        }
        return size;
    }

    public static int copyTo(ByteBuffer data, ByteBuffer dst, Field[] fields) {
        int written = 0;
        for (Field field : fields) {
            written += field.copyTo(data, dst);
        }
        return written;
    }

    public static int copyFrom(ByteBuffer data, ByteBuffer dst, Field[] fields) {
        int written = 0;
        for (Field field : fields) {
            written += field.copyFrom(data, dst);
        }
        return written;
    }

}

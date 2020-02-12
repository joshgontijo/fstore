package io.joshworks.ilog.fields;

import java.nio.ByteBuffer;

public class BlobField extends Field {

    public BlobField(int offset, int len) {
        super(b -> offset);
    }

    public BlobField(Mapper offsetSupplier, Mapper lenSupplier) {
        super(offsetSupplier);
    }

    public BlobField(int offset, Mapper lenSupplier) {
        this(b -> offset, lenSupplier);
    }

    public static BlobField after(Field field, Mapper offset) {
        return new BlobField(b -> afterOf(field, b), offset);
    }

    public int set(ByteBuffer fieldBuffer, ByteBuffer value) {
        return super.copyFrom(fieldBuffer, value);
    }

    @Override
    public int len(ByteBuffer b) {
        return 0;
    }
}

package io.joshworks.fstore.core.util;

public abstract class Size {

    public static final Size BYTE = new Size() {
        @Override
        public long of(int value) {
            return value;
        }

        @Override
        public int ofInt(int value) {
            return Size.toInt(of(value));
        }
    };
    private static final long BYTE_SCALE = 1L;
    private static final long KILOBYTE_SCALE = BYTE_SCALE * 1024L;
    public static final Size KB = new Size() {
        @Override
        public long of(int value) {
            return value * KILOBYTE_SCALE;
        }

        @Override
        public int ofInt(int value) {
            return Size.toInt(of(value));
        }
    };
    private static final long MEGABYTE_SCALE = KILOBYTE_SCALE * 1024L;
    public static final Size MB = new Size() {
        @Override
        public long of(int value) {
            return value * MEGABYTE_SCALE;
        }

        @Override
        public int ofInt(int value) {
            return Size.toInt(of(value));
        }
    };
    private static final long GIGABYTE_SCALE = MEGABYTE_SCALE * 1024L;
    public static final Size GB = new Size() {
        @Override
        public long of(int value) {
            return value * GIGABYTE_SCALE;
        }

        @Override
        public int ofInt(int value) {
            return Size.toInt(of(value));
        }
    };
    private static final long TERABYTE_SCALE = GIGABYTE_SCALE * 1024L;
    public static final Size TB = new Size() {
        @Override
        public long of(int value) {
            return value * TERABYTE_SCALE;
        }

        @Override
        public int ofInt(int value) {
            return Size.toInt(of(value));
        }
    };

    private static int toInt(long val) {
        if (val > Integer.MAX_VALUE) {
            throw new IllegalStateException(val + " > " + Integer.MAX_VALUE);
        }
        return (int) val;
    }

    public abstract long of(int value);

    public abstract int ofInt(int value);


}

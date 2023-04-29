package io.joshworks.es2.index;


public abstract class IndexFunction {

    public static final int NONE = -1;
    public static final IndexFunction EQUALS = new Equals();
    public static final IndexFunction FLOOR = new Floor();
    public static final IndexFunction CEILING = new Ceiling();
    public static final IndexFunction HIGHER = new Higher();
    public static final IndexFunction LOWER = new Lower();

    public abstract int apply(int idx);

    private static class Equals extends IndexFunction {

        @Override
        public int apply(int idx) {
            return Math.max(idx, NONE);
        }
    }

    private static class Floor extends IndexFunction {

        @Override
        public int apply(int idx) {
            return idx >= 0 ? idx : Math.abs(idx) - 2;
        }
    }

    private static class Ceiling extends IndexFunction {

        @Override
        public int apply(int idx) {
            return idx >= 0 ? idx : Math.abs(idx) - 1;
        }
    }

    private static class Lower extends IndexFunction {

        @Override
        public int apply(int idx) {
            return idx > 0 ? idx - 1 : Math.abs(idx) - 2;
        }
    }

    private static class Higher extends IndexFunction {

        @Override
        public int apply(int idx) {
            return idx >= 0 ? idx + 1 : Math.abs(idx) - 1;
        }
    }


}

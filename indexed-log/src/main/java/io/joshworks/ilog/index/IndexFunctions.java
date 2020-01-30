package io.joshworks.ilog.index;

import static io.joshworks.ilog.index.Index.NONE;

public abstract class IndexFunctions {

    abstract int apply(int idx);

    public static final IndexFunctions EQUALS = new Equals();
    public static final IndexFunctions FLOOR = new Floor();
    public static final IndexFunctions CEILING = new Ceiling();
    public static final IndexFunctions HIGHER = new Higher();
    public static final IndexFunctions LOWER = new Lower();

    private static class Equals extends IndexFunctions {

        @Override
        int apply(int idx) {
            return Math.max(idx, NONE);
        }
    }

    private static class Floor extends IndexFunctions {

        @Override
        int apply(int idx) {
            return idx >= 0 ? idx : Math.abs(idx) - 2;
        }
    }

    private static class Ceiling extends IndexFunctions {

        @Override
        int apply(int idx) {
            return idx >= 0 ? idx : Math.abs(idx) - 1;
        }
    }

    private static class Lower extends IndexFunctions {

        @Override
        int apply(int idx) {
            return idx > 0 ? idx - 1 : Math.abs(idx) - 2;
        }
    }

    private static class Higher extends IndexFunctions {

        @Override
        int apply(int idx) {
            return idx >= 0 ? idx + 1 : Math.abs(idx) - 1;
        }
    }


}

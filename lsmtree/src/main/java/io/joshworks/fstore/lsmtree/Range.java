package io.joshworks.fstore.lsmtree;

public class Range<K extends Comparable<K>> {

    private final K startInclusive;
    private final K endExclusive;

    private Range(K startInclusive, K endExclusive) {
        if (startInclusive != null && endExclusive != null && startInclusive.compareTo(endExclusive) > 0) {
            throw new IllegalArgumentException("Range end must greater or equals than Range start: [start=" + startInclusive + ", end=" + endExclusive + "]");
        }
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    public static <K extends Comparable<K>> Range<K> of(K startInclusive, K endExclusive) {
        return new Range<>(startInclusive, endExclusive);
    }

    public static <K extends Comparable<K>> Range<K> start(K startInclusive) {
        return new Range<>(startInclusive, null);
    }

    public static <K extends Comparable<K>> Range<K> end(K endExclusive) {
        return new Range<>(null, endExclusive);
    }

    public K start() {
        return startInclusive;
    }

    public K end() {
        return endExclusive;
    }

    public boolean inRange(K key) {
        boolean greaterOrEqualsThanStart = startInclusive == null || startInclusive.compareTo(key) <= 0;
        boolean lessThanEnd = endExclusive == null || endExclusive.compareTo(key) > 0;
        return greaterOrEqualsThanStart && lessThanEnd;
    }

    /**
     * Compare a given key with this range
     *
     * @return Negative number if this key is less than startInclusive, zero if the key greater or equals than
     * start and less than end, or positive when the key is greater than upper bound
     */
    public int compareTo(K value) {
        boolean greatOrEqualsThan = startInclusive == null || value.compareTo(startInclusive) >= 0;
        if (!greatOrEqualsThan) {
            return -1;
        }
        boolean lessThan = endExclusive == null || value.compareTo(endExclusive) < 0;
        if (!lessThan) {
            return 1;
        }
        return 0;
    }
}

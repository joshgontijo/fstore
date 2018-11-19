package io.joshworks.fstore.log.utils;

public class Pair<A, B> {

    public final A left;
    public final B right;

    private Pair(A left, B right) {
        this.left = left;
        this.right = right;
    }

    private static <A, B> Pair<A, B> of(A left, B right) {
        return new Pair<>(left, right);
    }

    public A left() {
        return left;
    }

    public B right() {
        return right;
    }
}

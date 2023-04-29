package io.joshworks.fstore.log.segment.footer;

public class FooterPosition {

    public long start;
    public long length;

    public FooterPosition() {
    }

    public FooterPosition(long start, long length) {
        this.start = start;
        this.length = length;
    }


    @Override
    public String toString() {
        return "FooterPosition{" +
                "start=" + start +
                ", length=" + length +
                '}';
    }
}

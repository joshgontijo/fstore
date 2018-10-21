package io.joshworks.fstore.core.metric;

public class Average {

    private int size;
    private double total = 0d;
    private int index = 0;
    private long[] samples;

    public Average() {
       this(100);
    }

    public Average(int size) {
        this.size = size;
        samples = new long[size];
        for (int i = 0; i < size; i++) samples[i] = 0;
    }

    public void add(long x) {
        total -= samples[index];
        samples[index] = x;
        total += x;
        if (++index == size) index = 0; // cheaper than modulus
    }

    public double average() {
        return (total / size);
    }
}
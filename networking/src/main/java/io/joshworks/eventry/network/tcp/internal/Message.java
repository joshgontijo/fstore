package io.joshworks.eventry.network.tcp.internal;

//Message that expected a response
public abstract class Message<T> {

    public final long id;
    public final boolean response;
    public final T data;

    protected Message(long id, boolean response, T data) {
        this.id = id;
        this.response = response;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Message{" + "id=" + id +
                ", response=" + response +
                ", data=" + data +
                '}';
    }
}

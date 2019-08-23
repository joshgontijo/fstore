package io.joshworks.eventry.network.tcp.internal;

/**
 * Message is used only for request-response scenarios, where a correlation must be carried along with the req / resp
 */
public class Message {

    public long id;
    public final Object data;

    public Message(long id, Object data) {
        this.id = id;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Message{" + "id=" + id +
                ", data=" + data +
                '}';
    }
}

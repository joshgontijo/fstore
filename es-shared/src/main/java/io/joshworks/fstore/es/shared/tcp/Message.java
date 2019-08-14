package io.joshworks.fstore.es.shared.tcp;

public abstract class Message {

    public static final long NO_RESP = 0;
    public long id;

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Message{");
        sb.append("id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

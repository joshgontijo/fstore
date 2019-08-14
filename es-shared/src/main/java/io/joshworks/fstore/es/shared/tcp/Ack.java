package io.joshworks.fstore.es.shared.tcp;

public class Ack extends Message {

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Ack{");
        sb.append("id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

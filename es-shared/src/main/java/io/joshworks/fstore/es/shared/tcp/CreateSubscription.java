package io.joshworks.fstore.es.shared.tcp;

public class CreateSubscription extends Message {

    public final String pattern;

    public CreateSubscription(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("CreateSubscription{");
        sb.append("pattern='").append(pattern).append('\'');
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

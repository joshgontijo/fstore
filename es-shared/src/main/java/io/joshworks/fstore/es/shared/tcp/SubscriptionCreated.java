package io.joshworks.fstore.es.shared.tcp;

public class SubscriptionCreated extends Message {

    public String subscriptionId;

    public SubscriptionCreated() {
    }

    public SubscriptionCreated(String id) {
        this.subscriptionId = id;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SubscriptionCreated{");
        sb.append("subscriptionId='").append(subscriptionId).append('\'');
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

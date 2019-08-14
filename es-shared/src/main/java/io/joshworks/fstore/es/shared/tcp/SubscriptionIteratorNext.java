package io.joshworks.fstore.es.shared.tcp;

public class SubscriptionIteratorNext extends Message {

    public final String subscriptionId;
    public final int batchSize;

    public SubscriptionIteratorNext(String subscriptionId, int batchSize) {
        this.subscriptionId = subscriptionId;
        this.batchSize = batchSize;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("SubscriptionIteratorNext{");
        sb.append("subscriptionId='").append(subscriptionId).append('\'');
        sb.append(", batchSize=").append(batchSize);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

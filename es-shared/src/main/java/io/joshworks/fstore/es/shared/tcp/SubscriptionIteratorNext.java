package io.joshworks.fstore.es.shared.tcp;

public class SubscriptionIteratorNext {

    public String subscriptionId;
    public int batchSize;

    public SubscriptionIteratorNext() {
    }

    public SubscriptionIteratorNext(String subscriptionId, int batchSize) {
        this.subscriptionId = subscriptionId;
        this.batchSize = batchSize;
    }
}

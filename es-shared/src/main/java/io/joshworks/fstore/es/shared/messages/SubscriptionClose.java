package io.joshworks.fstore.es.shared.messages;

public class SubscriptionClose {

    public String subscriptionId;

    public SubscriptionClose() {
    }

    public SubscriptionClose(String id) {
        this.subscriptionId = id;
    }
}

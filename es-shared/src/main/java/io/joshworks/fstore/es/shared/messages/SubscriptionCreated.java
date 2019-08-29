package io.joshworks.fstore.es.shared.messages;

public class SubscriptionCreated {

    public String subscriptionId;

    public SubscriptionCreated() {
    }

    public SubscriptionCreated(String id) {
        this.subscriptionId = id;
    }
}

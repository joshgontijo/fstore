package io.joshworks.fstore.es.shared.tcp;

public class SubscriptionCreated {

    public String subscriptionId;

    public SubscriptionCreated() {
    }

    public SubscriptionCreated(String id) {
        this.subscriptionId = id;
    }
}

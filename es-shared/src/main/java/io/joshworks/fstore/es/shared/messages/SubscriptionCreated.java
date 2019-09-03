package io.joshworks.fstore.es.shared.messages;

public class SubscriptionCreated {

    public String subscriptionId; //if null, then stream pattern(s) did not match the node

    public SubscriptionCreated() {
    }

    public SubscriptionCreated(String id) {
        this.subscriptionId = id;
    }
}

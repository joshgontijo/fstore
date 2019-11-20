package io.joshworks.fstore.server.subscription;

public class SubscriptionInfo {

    public final String subscriptionId;
    public final long since;
    public final long sent;
    public final boolean active;
    public final boolean open;
    //    public final long keepAlive; //TODO uncomment on snappy 0.6.1
    public final long lastSent;

    SubscriptionInfo(String subscriptionId, long since, long sent, boolean active, boolean open, long lastSent) {
        this.subscriptionId = subscriptionId;
        this.since = since;
        this.sent = sent;
        this.active = active;
        this.open = open;
//        this.keepAlive = keepAlive;
        this.lastSent = lastSent;
    }
}

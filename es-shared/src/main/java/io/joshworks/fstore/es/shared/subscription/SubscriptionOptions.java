package io.joshworks.fstore.es.shared.subscription;

import java.util.Set;

public class SubscriptionOptions {

    public final Set<String> patterns;
    public final int batchSize;
    public final boolean compress;
//    public final boolean wrapEvent; //NOT USED, CANNOT BE USED WITH BATCH

    public SubscriptionOptions(Set<String> patterns, int batchSize, boolean compress) {
        this.patterns = patterns;
        this.batchSize = batchSize;
        this.compress = compress;
    }
}

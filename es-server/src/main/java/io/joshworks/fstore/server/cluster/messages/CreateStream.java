package io.joshworks.fstore.server.cluster.messages;

import java.util.Map;

public class CreateStream {
    public final String stream;
    public final int maxCount;
    public final int maxAge;
    public final Map<String, Integer> acl;
    public final Map<String, String> metadata;

    public CreateStream(String stream, int maxCount, int maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        this.stream = stream;
        this.maxCount = maxCount;
        this.maxAge = maxAge;
        this.acl = acl;
        this.metadata = metadata;
    }
}

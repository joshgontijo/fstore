package io.joshworks.fstore.server.cluster.messages;

import java.util.Map;

public class StreamData  {
    public final String stream;
    public final int maxCount;
    public final  long maxAge;
    public final  Map<String, Integer> acl;
    public final  Map<String, String> metadata;

    public StreamData(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        this.stream = stream;
        this.maxCount = maxCount;
        this.maxAge = maxAge;
        this.acl = acl;
        this.metadata = metadata;
    }
}

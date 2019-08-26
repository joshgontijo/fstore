package io.joshworks.fstore.es.shared.tcp;

import java.util.Map;

public class CreateStream {

    public String name;
    public int maxAgeSec;
    public int maxCount;
    public Map<String, String> metadata;
    public Map<String, Integer> acl;

    public CreateStream() {
    }

    public CreateStream(String name) {
        this(name, 0, 0, null, null); //TODO USE CONSTANTS
    }

    public CreateStream(String name, int maxAgeSec, int maxCount, Map<String, Integer> acl, Map<String, String> metadata) {
        this.name = name;
        this.maxAgeSec = maxAgeSec;
        this.maxCount = maxCount;
        this.acl = acl;
        this.metadata = metadata;
    }
}

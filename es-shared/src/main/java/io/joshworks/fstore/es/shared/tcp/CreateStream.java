package io.joshworks.fstore.es.shared.tcp;

import java.util.Map;

public class CreateStream extends Message {

    public final String name;
    public final int maxAgeSec;
    public final int maxCount;
    public final Map<String, String> metadata;
    public final Map<String, Integer> acl;

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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("CreateStream{");
        sb.append("name='").append(name).append('\'');
        sb.append(", maxAgeSec=").append(maxAgeSec);
        sb.append(", maxCount=").append(maxCount);
        sb.append(", metadata=").append(metadata);
        sb.append(", acl=").append(acl);
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

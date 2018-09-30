package io.joshworks.eventry.projections;

import java.util.Set;

public class Projection {

    public final Set<String> streams;
    public final String script;
    public final String name;
    public final Type type;
    public final boolean enabled;

    public Projection(Set<String> streams, String script, String name, Type type, boolean enabled) {
        this.streams = streams;
        this.script = script;
        this.name = name;
        this.type = type;
        this.enabled = enabled;
    }


    public enum Type {
        CONTINOUS,
        ONE_TIME,
        AD_HOC

    }

    public enum State {
        ENABLED,
        DISABLED,
        NONE

    }

}

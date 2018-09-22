package io.joshworks.eventry.projections;

public class Projection {

    public final String script;
    public final String name;
    public final Type type;
    public final boolean enabled;

    public Projection(String script,  String name, Type type, boolean enabled) {
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

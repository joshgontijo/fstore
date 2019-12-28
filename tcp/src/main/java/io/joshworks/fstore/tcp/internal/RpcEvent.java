package io.joshworks.fstore.tcp.internal;

import java.util.Arrays;

public class RpcEvent {

    public final String methodName;
    public final Object[] params;

    public RpcEvent(String methodName, Object[] params) {
        this.methodName = methodName;
        this.params = params == null ? new Object[0] : params;
    }

    @Override
    public String toString() {
        return "RpcEvent{" +
                "methodName='" + methodName + '\'' +
                ", params=" + Arrays.toString(params) +
                '}';
    }
}

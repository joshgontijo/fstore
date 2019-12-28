package io.joshworks.fstore.cluster.rpc;

import java.util.Arrays;

public class RpcMessage {

    public final String methodName;
    public final Object[] params;

    public RpcMessage(String methodName, Object[] params) {
        this.methodName = methodName;
        this.params = params;
    }

    @Override
    public String toString() {
        return "RpcMessage{" +
                "methodName='" + methodName + '\'' +
                ", params=" + Arrays.toString(params) +
                '}';
    }
}

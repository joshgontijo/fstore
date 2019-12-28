package io.joshworks.fstore.cluster;

public class ClusterClientException extends RuntimeException {

    public ClusterClientException(String message) {
        super(message);
    }

    public ClusterClientException(String message, Throwable cause) {
        super(message, cause);
    }

}

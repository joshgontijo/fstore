package io.joshworks.fstore.cluster;

public class ErrorMessage {

    public final String code;
    public final String message;

    public ErrorMessage(String message) {
        this("NO_CODE", message);
    }

    public ErrorMessage(String code, String message) {
        this.code = code;
        this.message = message;
    }
}

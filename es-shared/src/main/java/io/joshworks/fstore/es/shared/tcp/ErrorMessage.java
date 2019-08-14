package io.joshworks.fstore.es.shared.tcp;

public class ErrorMessage extends Message {

    public final String message;

    public ErrorMessage(String message) {
        this.message = message;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ErrorMessage{");
        sb.append("message='").append(message).append('\'');
        sb.append(", id=").append(id);
        sb.append('}');
        return sb.toString();
    }
}

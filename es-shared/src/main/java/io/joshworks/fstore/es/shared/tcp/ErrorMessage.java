package io.joshworks.fstore.es.shared.tcp;

public class ErrorMessage extends Message {

    public String message;

    public ErrorMessage() {
    }

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

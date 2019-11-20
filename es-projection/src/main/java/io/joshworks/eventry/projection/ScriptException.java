package io.joshworks.fstore.projection;

public class ScriptException extends ProjectionException {

    ScriptException(String message) {
        super(message);
    }

    ScriptException(Exception e) {
        super(e);
    }

    ScriptException(String s, Exception e) {
        super(s, e);
    }
}

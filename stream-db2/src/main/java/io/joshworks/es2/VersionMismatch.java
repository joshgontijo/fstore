package io.joshworks.es2;

public class VersionMismatch extends RuntimeException {

    public VersionMismatch(long stream, int eventVersion, int streamVersion) {
        super("Version mismatch for stream " + stream + " event version: " + eventVersion + " stream version: " + streamVersion);
    }
}

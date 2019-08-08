package io.joshworks.fstore.es.shared;

public class NodeInfo {

    public final String id;
    public final String address;
    public final Status status;

    public NodeInfo(String id, String address, Status status) {
        this.id = id;
        this.address = address;
        this.status = status;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "id='" + id + '\'' +
                ", address='" + address + '\'' +
                ", status=" + status +
                '}';
    }
}

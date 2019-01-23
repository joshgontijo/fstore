package io.joshworks.eventry.server.cluster;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Cluster extends ReceiverAdapter {

    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private JChannel channel;
    private final String name;
    private View state;

    public Cluster(String name) {
        this.name = name;
    }

    public synchronized void join() {
        if (channel != null) {
            throw new RuntimeException("Already joined");
        }
        logger.info("Joining cluster");
        try {
            channel = new JChannel();
            channel.setReceiver(this);
            channel.setDiscardOwnMessages(true);
            channel.connect(name);
            channel.getState(null, 10000);
        } catch (Exception e) {
            throw new RuntimeException("Failed to join cluster", e);
        }
    }


    @Override
    public void viewAccepted(View view) {
        if (state != null) {
            for (Address address : view.getMembers()) {
                if (!state.containsMember(address)) {
                    System.out.println("Node joined: " + address);
                }
            }
            for (Address address : state.getMembers()) {
                if (!view.containsMember(address)) {
                    System.out.println("Node left: " + address);
                }
            }

        } else {
            for (Address address : view.getMembers()) {
                if (!this.channel.getAddress().equals(address)) {
                    System.out.println("Already connected nodes: " + address);
                }
//                    eventBus.emit(new NodeJoined(inetAddress(address)));
            }
        }
        state = view;
    }

    public static void main(String[] args) {
        new Cluster("yolo", 123).join();
    }

}

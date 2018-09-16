package io.joshworks.eventry.server.cluster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.eventbus.EventBus;

public class TcpServer {

    private final Server server;
    private final int port;
    private EventBus eventBus;

    public TcpServer(int port, EventBus eventBus) {
        this.port = port;
        this.eventBus = eventBus;
        this.server = new Server();

        Kryo kryo = server.getKryo();
        kryo.register(EventRecord.class);
        kryo.register(WelcomeMessage.class);
        kryo.register(byte[].class);
    }


    public void start() {
        try {
            server.addListener(new ServerListener());
            server.start();
            server.bind(port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private class ServerListener extends Listener {

        @Override
        public void connected(Connection connection) {
            super.connected(connection);
            System.out.println("Client connected");
        }

        @Override
        public void disconnected(Connection connection) {
            super.disconnected(connection);
        }

        @Override
        public void received(Connection connection, Object object) {
            if(object instanceof WelcomeMessage) {
                WelcomeMessage message = (WelcomeMessage) object;
                message.connection = connection;
                connection.sendTCP("Ok got your message: " + message.message);
            }

        }
    }

}

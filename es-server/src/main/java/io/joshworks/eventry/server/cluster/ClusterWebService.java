package io.joshworks.eventry.server.cluster;

import io.joshworks.snappy.websocket.WebsocketEndpoint;
import io.undertow.websockets.core.BufferedBinaryMessage;
import io.undertow.websockets.core.StreamSourceFrameChannel;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.spi.WebSocketHttpExchange;

import java.io.IOException;

public class ClusterWebService extends WebsocketEndpoint {

    private final ClusterManager clusterManager;

    public ClusterWebService(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }



    @Override
    public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {

    }

    @Override
    protected void onClose(WebSocketChannel webSocketChannel, StreamSourceFrameChannel channel) throws IOException {
        super.onClose(webSocketChannel, channel);
    }

    @Override
    protected void onFullBinaryMessage(WebSocketChannel channel, BufferedBinaryMessage message) throws IOException {
        super.onFullBinaryMessage(channel, message);
    }

    @Override
    protected void onFullCloseMessage(WebSocketChannel channel, BufferedBinaryMessage message) throws IOException {
        super.onFullCloseMessage(channel, message);
    }
}

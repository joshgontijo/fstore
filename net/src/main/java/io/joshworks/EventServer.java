package io.joshworks;

import io.joshworks.handlers.IdleHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class EventServer {

    public static final int LENGTH_FIELD_LENGTH = 4;
    private final ChannelFuture channel;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private EventServer(int port, int maxEventLength, int idleTimeout, EventHandler handler) {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        var server = new ServerBootstrap();
        server.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new ObjectEncoder())
                                .addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader())))
                                .addLast(new IdleStateHandler(idleTimeout, 0, 0, TimeUnit.MILLISECONDS))
                                .addLast(new IdleHandler())
//                                .addLast(new LengthFieldBasedFrameDecoder(maxEventLength, 0, LENGTH_FIELD_LENGTH, -LENGTH_FIELD_LENGTH, 0))
                                .addLast(handler);
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        try {
            this.channel = server.bind(port).sync();
        } catch (InterruptedException e) {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();

            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static EventServerBuilder create() {
        return new EventServerBuilder();
    }

    public void close() {
        try {
            channel.channel().closeFuture().sync();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static class EventServerBuilder {
        private int maxEventLength = Integer.MAX_VALUE;
        private BiConsumer<ChannelHandlerContext, Object> onMessage = (a, b) -> {
        };
        private BiConsumer<ChannelHandlerContext, Throwable> onError = (a, e) -> e.printStackTrace(System.err);
        private Consumer<ChannelHandlerContext> onConnect = a -> {
        };
        private Consumer<ChannelHandlerContext> onDisconnect = a -> {
        };

        private int idleTimeout;

        private EventServerBuilder() {

        }

        public EventServerBuilder onConnect(Consumer<ChannelHandlerContext> onConnect) {
            this.onConnect = requireNonNull(onConnect);
            return this;
        }

        public EventServerBuilder onDisconnect(Consumer<ChannelHandlerContext> onDisconnect) {
            this.onDisconnect = requireNonNull(onDisconnect);
            return this;
        }

        public EventServerBuilder onEvent(BiConsumer<ChannelHandlerContext, Object> onMessage) {
            this.onMessage = requireNonNull(onMessage);
            return this;
        }

        public EventServerBuilder onError(BiConsumer<ChannelHandlerContext, Throwable> onError) {
            this.onError = requireNonNull(onError);
            return this;
        }

        public EventServerBuilder maxEventLength(int maxEventLength) {
            this.maxEventLength = maxEventLength;
            return this;
        }

        public EventServerBuilder idleTimeout(int idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        public EventServer bind(int port) {
            return new EventServer(
                    port,
                    maxEventLength,
                    idleTimeout,
                    new EventHandler(
                            onMessage,
                            onError,
                            onConnect,
                            onDisconnect
                    )
            );
        }
    }

}

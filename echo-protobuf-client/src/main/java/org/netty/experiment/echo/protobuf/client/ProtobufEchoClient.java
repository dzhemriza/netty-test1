package org.netty.experiment.echo.protobuf.client;

import org.netty.experiment.protobuf.echo.stubs.ProtoEchoStubs;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client implementation
 */
public class ProtobufEchoClient {

    private static Logger LOG = Logger.getLogger(ProtobufEchoClient.class);
    private static final boolean SSL = System.getProperty("ssl") != null;
    private static final String HOST = System.getProperty("host", "127.0.0.1");
    private static final int PORT = Integer.parseInt(System.getProperty("port", "8123"));
    private static AtomicInteger messagesPerSecond = new AtomicInteger();

    private final static int NUMBER_OF_TIMES = 10000000;
    private final static int NUMBER_OF_CLIENTS = 5;

    private static void runImpl() throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        EventLoopGroup group = new NioEventLoopGroup(2);
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 4096)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                            }

                            //p.addLast(new ProtobufVarint32FrameDecoder());
                            p.addLast(new LengthFieldBasedFrameDecoder(83886080, 0, 4, 0, 4));
                            p.addLast(new ProtobufDecoder(ProtoEchoStubs.EchoResponse.getDefaultInstance()));

                            //p.addLast(new ProtobufVarint32LengthFieldPrepender());
                            p.addLast(new LengthFieldPrepender(4));
                            p.addLast(new ProtobufEncoder());

                            p.addLast(new ProtobufEchoClientHandler(messagesPerSecond, NUMBER_OF_TIMES));
                        }
                    });

            // Make a new connection.
            Channel ch = b.connect(HOST, PORT).sync().channel();

            ch.closeFuture().await();
        } finally {
            group.shutdownGracefully();
        }
    }


    private static void run() throws Exception {
        messagesPerSecond.set(0);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

        scheduledExecutorService.scheduleAtFixedRate((Runnable) () -> trackTime(), 1, 1, TimeUnit.SECONDS);

        ArrayList<Future<?>> concurrentClients = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CLIENTS);

        for (int i = 0; i < NUMBER_OF_CLIENTS; ++i) {
            concurrentClients.add(executorService.submit((Runnable)() -> {
                try {
                    runImpl();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }));
        }

        for (Future<?> task : concurrentClients) {
            task.get();
        }

        scheduledExecutorService.shutdown();
        executorService.shutdown();
        trackTime();
    }

    private static void trackTime() {
        int rate = messagesPerSecond.getAndSet(0);
        LOG.info("Messages per second: " + rate);
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            LOG.fatal(e.getMessage(), e);
        }
    }
}
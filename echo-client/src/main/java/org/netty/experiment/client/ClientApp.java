package org.netty.experiment.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ClientApp application
 */
public class ClientApp {

    private static final Logger LOG = Logger.getLogger(ClientApp.class);

    private static final int PORT = 8123;

    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int SIZE = 1024;//Integer.parseInt(System.getProperty("size", "1024"));
    static final boolean SSL = System.getProperty("ssl") != null;
    private static AtomicInteger messagesPerSecond = new AtomicInteger(0);

    private final static int NUMBER_OF_CLIENTS = 1;

    private static void run() throws Exception {
        // Configure SSL.git
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        // Configure the client.
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), HOST, PORT));
                            }
                            p.addLast(new EchoClientHandler(messagesPerSecond));
                        }
                    });

            // Start the client.
            ChannelFuture f = b.connect(HOST, PORT).sync();

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }

    private static void trackTime() {
        int rate = messagesPerSecond.getAndSet(0);
        LOG.info("Messages per second: " + rate);
    }

    private static void runImpl2() throws Exception {
        Socket socket = new Socket("localhost", 8123);

        try {
            LOG.trace(socket.isConnected());

            for (int i = 0; i < 10000000; ++i) {
                final String msg = "TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_TEST_DATA_";
                char[] data = msg.toCharArray();

                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(socket.getOutputStream());
                outputStreamWriter.write(data);
                outputStreamWriter.flush();

                InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                BufferedReader in = new BufferedReader(inputStreamReader);

                char[] echo = new char[data.length];

                int how = in.read(echo);

//                if (echo.length != data.length) {
//                    // do nothing
//                } else {
//                    LOG.error("not match");
//                }

                messagesPerSecond.incrementAndGet();
            }
        } finally {
            socket.close();
        }
    }

    public static void main(String[] args) {
        LOG.info("ClientApp started...");
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_CLIENTS);

        try {
            scheduledExecutorService.scheduleAtFixedRate((Runnable) () -> trackTime(), 1, 1, TimeUnit.SECONDS);

            ArrayList<Future<?>> concurrentClients = new ArrayList<>();

            for (int i = 0; i < NUMBER_OF_CLIENTS; ++i) {
                concurrentClients.add(executorService.submit((Runnable) () -> {
                    try {
                        run();
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }));
            }

            for (Future<?> task : concurrentClients) {
                task.get();
            }

        } catch (Exception e) {
            LOG.fatal(e.getMessage(), e);
        } finally {
            executorService.shutdown();
            scheduledExecutorService.shutdown();
        }
    }
}

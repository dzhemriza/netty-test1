package org.netty.experiment.server;

import org.apache.log4j.Logger;

/**
 * Application
 */
public class ServerApp {

    private static final Logger LOG = Logger.getLogger(ServerApp.class);
    private static final int PORT = 8123;

    public static void main(String[] args) {
        LOG.info("Server started...");

        try {
            Server server = new Server(PORT);
            server.run();
        } catch (Exception e) {
            LOG.fatal(e.getMessage(), e);
        }
    }
}

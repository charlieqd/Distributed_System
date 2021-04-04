package server;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;

public class TimeoutChecker extends Thread {

    private KVServer server;

    private static final long CHECK_MILLIS = 2000;

    private static Logger logger = Logger.getRootLogger();

    public TimeoutChecker(KVServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        while (server.isRunning()) {
            if (server.serving.get()) {
                server.checkLockTimeout();
            }
        }

        try {
            Thread.sleep(CHECK_MILLIS);
        } catch (InterruptedException e) {
            logger.error(e);
            return;
        }
    }
}

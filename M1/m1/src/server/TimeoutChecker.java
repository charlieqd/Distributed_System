package server;

import app_kvServer.KVServer;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class TimeoutChecker extends Thread {

    KVServer server;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static final long ChECK_MILLIS = 5000;

    private static Logger logger = Logger.getRootLogger();

    public TimeoutChecker(KVServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        while (running.get()) {

        }

        try {
            Thread.sleep(ChECK_MILLIS);
        } catch (InterruptedException e) {
            logger.error(e);
            return;
        }
    }
}

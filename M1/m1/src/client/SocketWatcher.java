package client;

import org.apache.log4j.Logger;
import shared.IProtocol;
import shared.Response;
import shared.Util;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

class SocketWatcher implements Runnable {
    private Logger logger = Logger.getRootLogger();

    private IProtocol protocol;
    private BlockingQueue<Response> watcherQueue;
    private ServerConnection connection;

    public SocketWatcher(IProtocol protocol,
                         BlockingQueue<Response> watcherQueue,
                         ServerConnection connection) {
        this.protocol = protocol;
        this.watcherQueue = watcherQueue;
        this.connection = connection;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Response res = protocol.readResponse(connection.input);
                watcherQueue.put(res);
            } catch (IOException | InterruptedException e) {
                logger.info("Socket closed.");
                try {
                    watcherQueue.put(new Response(new byte[0], -1,
                            Response.Status.DISCONNECTED));
                } catch (InterruptedException interruptedException) {
                    logger.error(
                            Util.getStackTraceString(interruptedException));
                }
                connection.terminated.set(true);
                break;
            }
        }
    }
}

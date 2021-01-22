package testing;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;
import shared.Protocol;
import shared.messages.KVMessageSerializer;

import java.io.IOException;


public class AllTests {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            new KVServer(new Protocol(), new KVMessageSerializer(), 50000, 10,
                    IKVServer.CacheStrategy.FIFO);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class);
        clientSuite.addTestSuite(InteractionTest.class);
        // clientSuite.addTestSuite(AdditionalTest.class);
        return clientSuite;
    }

}

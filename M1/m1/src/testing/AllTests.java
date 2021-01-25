package testing;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import server.KVStorage;
import server.MD5PrefixKeyHashStrategy;
import shared.Protocol;
import shared.messages.KVMessageSerializer;


@RunWith(Suite.class)
@Suite.SuiteClasses({ConnectionTest.class, InteractionTest.class, AdditionalTest.class, ApplicationTest.class})
public class AllTests {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void setUp() {
        try {
            String rootPath = folder.newFolder("data").toString();
            new LogSetup("logs/testing/test.log", Level.ERROR);
            new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.FIFO), new Protocol(),
                    new KVMessageSerializer(), 50000).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

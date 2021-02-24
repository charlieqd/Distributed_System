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
import shared.ECSNode;
import shared.Metadata;
import shared.Protocol;
import shared.messages.KVMessageSerializer;

import java.util.Arrays;


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
            KVServer server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50000, "testServer", null);
            server.start();
            server.updateMetadata(new Metadata(
                    Arrays.asList(
                            new ECSNode("testServer", "127.0.0.1", 50000))));
            server.startServing();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

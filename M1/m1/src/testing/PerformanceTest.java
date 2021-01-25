package testing;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import server.IKVStorage;
import server.KVStorage;
import server.MD5PrefixKeyHashStrategy;
import shared.Protocol;
import shared.messages.KVMessageSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Parameterized.class)
public class PerformanceTest {

    private static final float TOTAL_DURATION_MILLIS = 10000;
    private static final float WARM_UP_TIME_MILLIS = 1000;
    private static final int KEY_COUNT = 1000;
    private static final int CACHE_SIZE = 100;

    private static final Random random = new Random();

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();
    public static IKVStorage storage = null;

    private KVStore kvStore;
    private final float putRatio;
    private final float getRatio;

    @BeforeClass
    public static void init() {
        try {
            System.out.println("Setting up server...");

            String rootPath = folder.newFolder("perfdata").toString();
            new LogSetup("logs/testing/test.log", Level.ERROR);
            storage = new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                    CACHE_SIZE, IKVServer.CacheStrategy.LRU);
            new KVServer(
                    storage, new Protocol(),
                    new KVMessageSerializer(), 50000).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws IOException {
        System.out.println("Resetting data...");

        for (int i = 0; i < KEY_COUNT; ++i) {
            storage.put(Integer.toString(i), getRandomValue());
        }
        storage.clearCache();

        System.out.println("Setting up client...");

        kvStore = new KVStore("localhost", 50000);
        try {
            kvStore.connect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        System.out.println("Disconneting...");

        kvStore.disconnect();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {20, 80}, {50, 50}, {80, 20}
        });
    }

    public PerformanceTest(float putRatio, float getRatio) {
        this.putRatio = putRatio;
        this.getRatio = getRatio;
    }

    @Test
    public void profilePerformance() throws Exception {
        float putProbability = (putRatio + getRatio) == 0 ? 1 :
                Math.min(Math.max(putRatio / (putRatio + getRatio), 0), 1);

        System.out.println(
                "PUT probability = " + putProbability +
                        " (PUT:GET = " + putRatio + ":" + getRatio + ")");

        int numResponses = 0;
        long startTime = getTimeMs();
        long currentTime;
        long latencySum = 0;

        System.out.println("Measuring...");

        do {
            currentTime = getTimeMs();

            long start = getTimeMs();
            if (random.nextFloat() < putProbability) {
                kvStore.put(getRandomKey(), getRandomValue());
            } else {
                kvStore.get(getRandomKey());
            }
            long end = getTimeMs();

            if (currentTime - startTime > WARM_UP_TIME_MILLIS) {
                numResponses += 1;
                latencySum += end - start;
            }
        } while (currentTime - startTime < TOTAL_DURATION_MILLIS);

        float averageLatency =
                numResponses == 0 ? 0 :
                        ((float) latencySum) / 1000 / numResponses;
        float throughput = ((float) numResponses) / TOTAL_DURATION_MILLIS * 1000;

        System.out.println(
                "Average latency (seconds): " + averageLatency +
                        ", Throughput (per second): " + throughput);
    }

    private static String getRandomKey() {
        return Integer.toString(random.nextInt(KEY_COUNT));
    }

    private static String getRandomValue() {
        return Integer.toString(random.nextInt(1000000000));
    }

    private static long getTimeMs() {
        return System.currentTimeMillis();
    }
}

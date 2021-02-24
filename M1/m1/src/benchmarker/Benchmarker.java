package benchmarker;

import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.Random;

/**
 * This program creates multiple client KVStore and repeated sends request to
 * the distributed nodes. The latency and throughput will be measured and
 * reported. The servers (including ECS and ZooKeeper) must be correctly setup
 * first before running this program.
 */
public class Benchmarker {

    public static final float PUT_PROBABILITY = 0.5F;

    private static final float TOTAL_DURATION_MILLIS = 10000;
    private static final float WARM_UP_TIME_MILLIS = 1000;

    private static class BenchmarkerClient extends Thread {
        private final Random random = new Random();

        private final KVStore kvStore;

        public BenchmarkerClient(String host, int port) throws Exception {
            kvStore = new KVStore(host, port);
            try {
                kvStore.connect();
            } catch (Exception e) {
                kvStore.disconnect();
                throw e;
            }
        }

        @Override
        public void run() {
            try {
                int numResponses = 0;
                long startTime = getTimeMs();
                long currentTime;
                long latencySum = 0;

                do {
                    currentTime = getTimeMs();

                    long start = getTimeMs();
                    if (random.nextFloat() < PUT_PROBABILITY) {
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
                float throughput = ((float) numResponses) /
                        (TOTAL_DURATION_MILLIS - WARM_UP_TIME_MILLIS) * 1000;

                // TODO change this
                System.out.println(
                        "Average latency (seconds): " + averageLatency +
                                ", Throughput (per second): " + throughput);
            } catch (Exception e) {
                // TODO report this
            } finally {
                kvStore.disconnect();
            }
        }

        private String getRandomValue() {
            throw new Error("Not implemented");
        }

        private String getRandomKey() {
            throw new Error("Not implemented");
        }
    }

    public static void main(String[] args) throws IOException {
        new LogSetup("logs/benchmarker.log", Level.ERROR);
        System.out.println("Implement me");
    }

    private static long getTimeMs() {
        return System.currentTimeMillis();
    }
}

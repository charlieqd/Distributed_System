package benchmarker;

import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;
import shared.messages.KVMessage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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

    private static class BenchmarkResult {
        /**
         * Average latency in seconds.
         */
        public AtomicReference<Float> latency =
                new AtomicReference<Float>(-1f);
        /**
         * Throughput in operations per second.
         */
        public AtomicReference<Float> throughput =
                new AtomicReference<Float>(-1f);
        /**
         * Error message if applicable.
         */
        public AtomicReference<String> errorMessage =
                new AtomicReference<String>(null);
    }

    private static class BenchmarkerClient extends Thread {
        private final Random random = new Random();

        private final KVStore kvStore;
        private final BenchmarkResult result;
        private final int maxKey;
        private final int maxValue;

        private final Set<KVMessage.StatusType> ACCEPTABLE_RESPONSE_STATUSES = new HashSet<>(
                Arrays.asList(KVMessage.StatusType.PUT_UPDATE,
                        KVMessage.StatusType.PUT_SUCCESS,
                        KVMessage.StatusType.GET_SUCCESS,
                        KVMessage.StatusType.GET_ERROR));

        public BenchmarkerClient(String host,
                                 int port,
                                 int maxKey,
                                 int maxValue) throws Exception {
            this.maxKey = maxKey;
            this.maxValue = maxValue;
            result = new BenchmarkResult();
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
                    KVMessage response;
                    if (random.nextFloat() < PUT_PROBABILITY) {
                        response = kvStore
                                .put(getRandomKey(), getRandomValue());
                    } else {
                        response = kvStore.get(getRandomKey());
                    }
                    if (!ACCEPTABLE_RESPONSE_STATUSES
                            .contains(response.getStatus())) {
                        result.errorMessage.set(String
                                .format("Server responded with status %s",
                                        response.getStatus().name()));
                        return;
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

                result.latency.set(averageLatency);
                result.throughput.set(throughput);
            } catch (Exception e) {
                // TODO report this
            } finally {
                kvStore.disconnect();
            }
        }

        private String getRandomKey() {
            return Integer.toString(random.nextInt(maxKey));
        }

        private String getRandomValue() {
            return Integer.toString(random.nextInt(maxValue));
        }

        public BenchmarkResult getResult() {
            return result;
        }

        private static long getTimeMs() {
            return System.currentTimeMillis();
        }
    }

    public Benchmarker() {
    }

    public void run(String[] args) throws Exception {
        if (args.length != 5) {
            printUsage();
            return;
        }
        int numClients = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int maxKey = Integer.parseInt(args[3]);
        int maxValue = Integer.parseInt(args[4]);
        BenchmarkerClient[] clients = new BenchmarkerClient[numClients];

        System.out.printf("Starting %d clients to connect to %s:%d...\n",
                numClients, host, port);

        System.out.printf("Max key: %d, max value: %d\n", maxKey, maxValue);

        for (int i = 0; i < numClients; ++i) {
            clients[i] = new BenchmarkerClient(host, port, maxKey, maxValue);
            clients[i].start();
        }

        float averageLatency = 0;
        float averageThroughput = 0;
        int validSampleCount = 0;

        for (BenchmarkerClient client : clients) {
            client.join();
            BenchmarkResult result = client.getResult();
            float latency = result.latency.get();
            float throughput = result.throughput.get();
            String errorMessage = result.errorMessage.get();
            if (errorMessage != null) {
                System.out.println("Client error: " + errorMessage);
                continue;
            }
            if (latency >= 0 && throughput >= 0) {
                validSampleCount++;
                averageLatency += latency;
                averageThroughput += throughput;
            }
        }

        System.out.printf("%d/%d clients successfully finished.\n",
                validSampleCount, numClients);

        if (validSampleCount > 0) {
            averageLatency /= validSampleCount;
            averageThroughput /= validSampleCount;
            System.out.println(
                    "Average latency (seconds): " + averageLatency +
                            ", Throughput (per second): " + averageThroughput);
        }
    }

    public static void main(String[] args) throws Exception {
        new LogSetup("logs/benchmarker.log", Level.ERROR);
        new Benchmarker().run(args);
    }

    private void printUsage() {
        System.out.println("usage: numClients host port maxKey maxValue");
    }
}

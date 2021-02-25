package benchmarker;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.FileWriter;
import client.KVStore;
import logger.LogSetup;
import org.apache.log4j.Level;

public class PopulateData {

    private static KVStore kvStore;
    private static String host;
    private static int port;
    private static int count = 0;

    public static void setUp() throws IOException {
        kvStore = new KVStore(host, port);
        System.out.println("host is " + host);
        System.out.println("port is " + port);
        try {
            kvStore.connect();
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("Connection failed");
        }
    }

    public static void tearDown() {
        kvStore.disconnect();
    }

    public static void listFilesForFolder(final File folder) throws
            Exception {
        for (final File fileEntry : folder.listFiles()) {
            if(count > 10000)
                return;
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                String filePath = fileEntry.getPath();
                String content = readFile(filePath, StandardCharsets.UTF_8);
                content = content.substring(0,100);
                System.out.println(count);
                kvStore.put(Integer.toString(count), content);
                count += 1;
                if(count > 10000)
                    return;
            }
        }
    }

    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static void main(String[] args) throws Exception {
        new LogSetup("logs/populateData.log", Level.ERROR);
        String readingFilename = args[0];
        host = args[1];
        port = Integer.parseInt(args[2]);
        setUp();
        final File folder = new File(readingFilename);
        listFilesForFolder(folder);
        tearDown();
    }

}

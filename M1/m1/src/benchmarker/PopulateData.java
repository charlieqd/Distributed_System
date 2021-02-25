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
    private static ArrayList<String> keys = new ArrayList<>();

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
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                String filePath = fileEntry.getPath();
                System.out.println(filePath);
                String fileKey = filePath;
                if(filePath.length() > 19){
                    fileKey = filePath.substring(filePath.length() - 19);
                }
                keys.add(fileKey);
                String content = readFile(filePath, StandardCharsets.UTF_8);
                if(content.length() > 120000){
                    content = content.substring(0,120000);
                }
                kvStore.put(fileKey, content);
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
        String keyFileName = args[1];
        host = args[2];
        port = Integer.parseInt(args[3]);
        setUp();
        final File folder = new File(readingFilename);
        listFilesForFolder(folder);
        System.out.println(keys.size());
        FileWriter writer = new FileWriter(keyFileName);
        for(String str: keys) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
        tearDown();
    }

}

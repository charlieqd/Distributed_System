package testing;

import app_kvClient.KVClient;
import client.KVStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.stream.Stream;

public class ApplicationTest {

    private KVStore kvClient;
    private KVClient app;
    private Stream<String> stream;
    private Stream.Builder<String> builder;

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final PrintStream originalErr = System.err;
    
    @Before
    public void setUp() throws Exception {
        kvClient = new KVStore("localhost", 50000);
        builder = Stream.builder();
//        Stream<String> stream = builder.add("Geeks").build();
        app = new KVClient((InputStream) stream);
        try {
            kvClient.connect();
            app.run();
        } catch (Exception e) {
        }
    }

    @After
    public void closeApp() {
        stream = builder.add("quit").build();
    }


    @Test
    public void testApp() {
        String cmd = "put a 1";
        stream = builder.add(cmd).build();

    }
}

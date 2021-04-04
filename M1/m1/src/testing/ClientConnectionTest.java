package testing;

import app_kvClient.KVClient;
import client.KVStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ClientConnectionTest {

    private KVClient app;
    private ByteArrayInputStream inputS;
    private final PrintStream standardOut = System.out;
    private ByteArrayOutputStream outputStreamCaptor;


    @Before
    public void setUp() throws Exception {
        outputStreamCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStreamCaptor));
    }

    @After
    public void closeApp() {
        System.setOut(standardOut);
    }

    @Test
    public void testConnectionSuccess() throws IOException {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }



}

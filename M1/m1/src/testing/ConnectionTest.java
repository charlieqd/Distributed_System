package testing;

import client.KVStore;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ConnectionTest {

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

    @Test
    public void testUnknownHost() throws IOException {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", 50000);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }

    @Test
    public void testIllegalPort() throws IOException {
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }


}

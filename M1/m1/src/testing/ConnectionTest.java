package testing;

import client.KVStore;
import junit.framework.TestCase;

import java.io.IOException;
import java.net.UnknownHostException;


public class ConnectionTest extends TestCase {


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

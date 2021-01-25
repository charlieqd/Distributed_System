package testing;

import client.KVStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class InteractionTest {

    private KVStore kvStore;

    @Before
    public void setUp() throws IOException {
        kvStore = new KVStore("localhost", 50000);
        try {
            kvStore.connect();
        } catch (Exception e) {
        }
    }

    @After
    public void tearDown() {
        kvStore.disconnect();
    }

    @Test
    public void testPut() {
        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvStore.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(
                ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }

    @Test
    public void testPutDisconnected() {
        kvStore.disconnect();
        String key = "foo";
        String value = "bar";
        Exception ex = null;

        try {
            kvStore.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }

    @Test
    public void testUpdate() {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvStore.put(key, initialValue);
            response = kvStore.put(key, updatedValue);

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
                && response.getValue().equals(updatedValue));
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvStore.put(key, value);
            response = kvStore.put(key, null);

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response
                .getStatus() == StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvStore.put(key, value);
            response = kvStore.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvStore.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }


}

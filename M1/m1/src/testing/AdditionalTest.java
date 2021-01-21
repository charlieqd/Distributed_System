package testing;

import junit.framework.TestCase;
import org.junit.Test;
import server.LRUCache;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.IOException;

public class AdditionalTest extends TestCase {

    // TODO add your test cases, at least 3

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        KVMessageSerializer s = new KVMessageSerializer();
        KVMessage m = new KVMessageImpl("key", null, KVMessage.StatusType.PUT);
        KVMessage m2 = s.decode(s.encode(m));
        assertEquals(m.getKey(), m2.getKey());
        assertEquals(m.getValue(), m2.getValue());
        assertEquals(m.getStatus(), m2.getStatus());
    }

    @Test
    public void testLRUCache() throws IOException, ClassNotFoundException {
        LRUCache LRU = new LRUCache(2);
        LRU.set("address", "toronto");
        assertEquals(LRU.get("address"), "toronto");
        LRU.set("name", "Alice");
        LRU.set("age", "20");
        assertNull(LRU.get("address"));
        assertEquals(LRU.get("name"), "Alice");
        assertEquals(LRU.get("age"), "20");

    }
}

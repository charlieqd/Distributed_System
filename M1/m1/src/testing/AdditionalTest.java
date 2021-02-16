package testing;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import server.FIFOCache;
import server.KVFileStorage;
import server.LRUCache;
import shared.ECSNode;
import shared.Metadata;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AdditionalTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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
        LRU.put("address", "toronto");
        LRU.put("name", "Alice");
        assertEquals(LRU.get("address"), "toronto");
        LRU.put("age", "20");
        assertNull(LRU.get("name"));
        assertEquals(LRU.get("address"), "toronto");
        assertEquals(LRU.get("age"), "20");

    }

    @Test
    public void testFIFOCache() throws IOException, ClassNotFoundException {
        FIFOCache FIFO = new FIFOCache(2);
        FIFO.put("address", "toronto");
        assertEquals(FIFO.get("address"), "toronto");
        FIFO.put("name", "Alice");
        FIFO.put("age", "20");
        assertNull(FIFO.get("address"));
        assertEquals(FIFO.get("name"), "Alice");
        assertEquals(FIFO.get("age"), "20");

    }

    @Test
    public void testCSVStringEscape() {
        {
            String a = "abcdef", b = "abcdef";
            assertEquals(b, Util.escapeCSVString(a));
            assertEquals(a, Util.unescapeCSVString(b));
        }
        {
            String a = "\\a,bc\ndef\r\n", b = "\\\\a\\,bc\\ndef\\r\\n";
            assertEquals(b, Util.escapeCSVString(a));
            assertEquals(a, Util.unescapeCSVString(b));
        }
        {
            String a = "\\,,,\\na", b = "\\\\\\,\\,\\,\\\\na";
            assertEquals(b, Util.escapeCSVString(a));
            assertEquals(a, Util.unescapeCSVString(b));
        }
    }

    @Test
    public void testCSVStringSplit() {
        {
            String line = "a\\,b\\nc,\\\\def\\r";
            String[] expected = new String[]{"a,b\nc", "\\def\r"};
            List<String> actual = Util.csvSplitLine(line);
            assertEquals(expected.length, actual.size());
            assertEquals(expected[0], actual.get(0));
            assertEquals(expected[1], actual.get(1));
        }
        {
            String line = "abcdef";
            String[] expected = new String[]{"abcdef"};
            List<String> actual = Util.csvSplitLine(line);
            assertEquals(expected.length, actual.size());
            assertEquals(expected[0], actual.get(0));
        }
        {
            String line = "ab,cdef,";
            String[] expected = new String[]{"ab", "cdef", ""};
            List<String> actual = Util.csvSplitLine(line);
            assertEquals(expected.length, actual.size());
            assertEquals(expected[0], actual.get(0));
            assertEquals(expected[1], actual.get(1));
            assertEquals(expected[2], actual.get(2));
        }
        {
            String line = "";
            String[] expected = new String[]{""};
            List<String> actual = Util.csvSplitLine(line);
            assertEquals(expected.length, actual.size());
            assertEquals(expected[0], actual.get(0));
        }
    }

    @Test
    public void testKVFileStorage() throws IOException {
        File createdFile = folder.newFile("testKVFileStorage.txt");
        KVFileStorage storage = new KVFileStorage(createdFile.getPath());
        storage.write("address", "\ntoronto");
        storage.write("name", "Alice, Wang");
        storage.write("ag\\e", "20");
        assertEquals(storage.read("address"), "\ntoronto");
        assertEquals(storage.read("name"), "Alice, Wang");
        assertEquals(storage.read("ag\\e"), "20");
        storage.write("address", null);
        assertNull(storage.read("address"));
        storage.write("name", "Jo,e");
        assertEquals(storage.read("name"), "Jo,e");
        storage.write("name", null);
        storage.write("ag\\e", null);
        assertNull(storage.read("name"));
        assertNull(storage.read("ag\\e"));
        storage.write(",,,,", "//\\/\\/\\/\\/");
        assertEquals(storage.read(",,,,"), "//\\/\\/\\/\\/");
    }

    @Test
    public void testMetaData() {
        ECSNode s1 = new ECSNode("s1", "ip", 0, "apple");
        ECSNode s2 = new ECSNode("s2", "ip", 0, "beta");
        ECSNode s3 = new ECSNode("s3", "ip", 0, "cat");
        ECSNode s4 = new ECSNode("s4", "ip", 0, "dog");
        ArrayList<ECSNode> servers = new ArrayList<ECSNode>();
        servers.add(s1);
        servers.add(s2);
        servers.add(s3);
        servers.add(s4);
        Metadata metadata = new Metadata(servers);
        assertEquals(s1, metadata.getServer("eye"));
        assertEquals(s1, metadata.getServer("aa"));
        assertEquals(s2, metadata.getServer("bad"));
        assertEquals(s3, metadata.getServer("bob"));
        assertEquals(s4, metadata.getServer("cici"));
        assertEquals(s1, metadata.getServer("apple"));
        assertEquals(s2, metadata.getServer("beta"));
        assertEquals(s3, metadata.getServer("cat"));
        assertEquals(s4, metadata.getServer("dog"));

        servers.clear();
        assertNull(metadata.getServer("eye"));

        servers.add(s1);
        assertEquals(s1, metadata.getServer("eye"));
        assertEquals(s1, metadata.getServer("aa"));
        assertEquals(s1, metadata.getServer("bad"));


    }
}

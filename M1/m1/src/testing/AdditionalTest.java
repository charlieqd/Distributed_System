package testing;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import client.ServerConnection;
import ecs.ECSController;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import server.*;
import shared.ECSNode;
import shared.Metadata;
import shared.Protocol;
import shared.Util;
import shared.messages.KVMessage;
import shared.messages.KVMessageImpl;
import shared.messages.KVMessageSerializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

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
        storage.write("ag\\e", "");
        assertEquals(storage.read("address"), "\ntoronto");
        assertEquals(storage.read("name"), "Alice, Wang");
        assertEquals(storage.read("ag\\e"), "");
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
    public void testMetadataGetServer() {
        ECSNode s1 = new ECSNode("s1", "ip", 0, "apple");
        ECSNode s2 = new ECSNode("s2", "ip", 0, "beta");
        ECSNode s3 = new ECSNode("s3", "ip", 0, "cat");
        ECSNode s4 = new ECSNode("s4", "ip", 0, "dog");
        List<ECSNode> servers = Arrays.asList(s2, s1, s4, s3);
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

        servers = new ArrayList<>();
        metadata = new Metadata(servers);
        assertNull(metadata.getServer("eye"));

        servers = Arrays.asList(s1);
        metadata = new Metadata(servers);
        assertEquals(s1, metadata.getServer("eye"));
        assertEquals(s1, metadata.getServer("aa"));
        assertEquals(s1, metadata.getServer("bad"));
    }

    @Test
    public void testMetadataGetRingPosition() {
        assertEquals("0cc175b9c0f1b6a831c399e269772661",
                Metadata.getRingPosition("a"));
        assertEquals("92eb5ffee6ae2fec3ad71c777531578f",
                Metadata.getRingPosition("b"));
        assertEquals("4a8a08f09d37b73795649038408b5f33",
                Metadata.getRingPosition("c"));
        assertEquals("e2a0f12f0779da1e546a8ff3720d74ac",
                Metadata.getRingPosition("ece419"));
        assertEquals("d41d8cd98f00b204e9800998ecf8427e",
                Metadata.getRingPosition(""));
        for (int i = 0; i < 100; ++i) {
            String s1 = Metadata.getRingPosition(Integer.toString(i));
            String s2 = Metadata.getRingPosition(Integer.toString(i));
            assertEquals(s1, s2);
        }
    }

    @Test
    public void testMetadataGetPredecessor() {
        ECSNode s1 = new ECSNode("s1", "ip", 0, "apple");
        ECSNode s2 = new ECSNode("s2", "ip", 0, "beta");
        ECSNode s3 = new ECSNode("s3", "ip", 0, "cat");
        ECSNode s4 = new ECSNode("s4", "ip", 0, "dog");
        ECSNode s5 = new ECSNode("s5", "ip", 0, "hi");
        List<ECSNode> servers = Arrays.asList(s2, s1, s4, s3);
        Metadata metadata = new Metadata(servers);

        assertEquals(s4, metadata.getPredecessor(s1));
        assertEquals(s1, metadata.getPredecessor(s2));
        assertEquals(s2, metadata.getPredecessor(s3));
        assertEquals(s3, metadata.getPredecessor(s4));
        assertNull(metadata.getPredecessor(s5));

        metadata = new Metadata(new ArrayList<>());
        assertNull(metadata.getPredecessor(s5));

        servers = Arrays.asList(s1);
        metadata = new Metadata(servers);
        assertEquals(s1, metadata.getPredecessor(s1));
    }

    @Test
    public void testMetadataGetSuccessor() {
        ECSNode s1 = new ECSNode("s1", "ip", 0, "apple");
        ECSNode s2 = new ECSNode("s2", "ip", 0, "beta");
        ECSNode s3 = new ECSNode("s3", "ip", 0, "cat");
        ECSNode s4 = new ECSNode("s4", "ip", 0, "dog");
        ECSNode s5 = new ECSNode("s5", "ip", 0, "hi");
        List<ECSNode> servers = Arrays.asList(s2, s1, s4, s3);
        Metadata metadata = new Metadata(servers);

        assertEquals(s2, metadata.getSuccessor(s1));
        assertEquals(s3, metadata.getSuccessor(s2));
        assertEquals(s4, metadata.getSuccessor(s3));
        assertEquals(s1, metadata.getSuccessor(s4));
        assertNull(metadata.getSuccessor(s5));

        metadata = new Metadata(new ArrayList<>());
        assertNull(metadata.getSuccessor(s5));

        servers = Arrays.asList(s1);
        metadata = new Metadata(servers);
        assertEquals(s1, metadata.getSuccessor(s1));
    }

    @Test
    public void testECSReadConfig() throws IOException {
        File configFile = folder.newFile("testecs.config");
        try (BufferedWriter bw = new BufferedWriter(
                new FileWriter(configFile))) {
            bw.write("a 127.0.0.1 123\n");
            bw.write("b 127.0.0.2 345\n");
            bw.write("c 127.0.0.3 678\n");
        }
        ArrayList<ECSNode> nodes = ECSController
                .readConfig(configFile.getPath());
        assertEquals(3, nodes.size());
        assertEquals("a", nodes.get(0).getNodeName());
        assertEquals("b", nodes.get(1).getNodeName());
        assertEquals("c", nodes.get(2).getNodeName());
        assertEquals("127.0.0.1", nodes.get(0).getNodeHost());
        assertEquals("127.0.0.2", nodes.get(1).getNodeHost());
        assertEquals("127.0.0.3", nodes.get(2).getNodeHost());
        assertEquals(123, nodes.get(0).getNodePort());
        assertEquals(345, nodes.get(1).getNodePort());
        assertEquals(678, nodes.get(2).getNodePort());
    }

    @Test
    public void testKVStorageGetAllKeys() throws NoSuchAlgorithmException,
            IOException {
        String rootDir = folder.newFolder("getallkeystest").toString();
        KVStorage storage = new KVStorage(rootDir,
                new MD5PrefixKeyHashStrategy(1), 100,
                IKVServer.CacheStrategy.LRU);
        storage.put("a", "");
        storage.put("b", "");
        storage.put("c", "");
        storage.put("ece419", "");
        storage.put("", "");
        Set<String> keys1 = new HashSet<String>(
                storage.getAllKeys("a0000000000000000000000000000000",
                        "10000000000000000000000000000000"));
        assertEquals(keys1,
                new HashSet<String>(Arrays.asList("ece419", "", "a")));
        Set<String> keys2 = new HashSet<String>(
                storage.getAllKeys("10000000000000000000000000000000",
                        "a0000000000000000000000000000000"));
        assertEquals(keys2,
                new HashSet<String>(Arrays.asList("b", "c")));
        Set<String> keys3 = new HashSet<String>(
                storage.getAllKeys("0cc175b9c0f1b6a831c399e269772661",
                        "92eb5ffee6ae2fec3ad71c777531578f"));
        assertEquals(keys3,
                new HashSet<String>(Arrays.asList("b", "c")));
    }

    @Test
    public void testKVFileStorageReadKeys() throws IOException {
        KVFileStorage fileStorage = new KVFileStorage(
                folder.newFile().toString());
        fileStorage.write("a", "0");
        fileStorage.write("b", "1");
        fileStorage.write("c", "2");
        fileStorage.write("ece419", "3");
        fileStorage.write("", "4");
        Set<String> keys1 = new HashSet<String>(
                fileStorage.readKeys("a0000000000000000000000000000000",
                        "10000000000000000000000000000000"));
        assertEquals(keys1,
                new HashSet<String>(Arrays.asList("ece419", "", "a")));
        Set<String> keys2 = new HashSet<String>(
                fileStorage.readKeys("10000000000000000000000000000000",
                        "a0000000000000000000000000000000"));
        assertEquals(keys2,
                new HashSet<String>(Arrays.asList("b", "c")));
        Set<String> keys3 = new HashSet<String>(
                fileStorage.readKeys("0cc175b9c0f1b6a831c399e269772661",
                        "92eb5ffee6ae2fec3ad71c777531578f"));
        assertEquals(keys3,
                new HashSet<String>(Arrays.asList("b", "c")));
    }

    @Test
    public void testKVServerShutdown() throws IOException,
            NoSuchAlgorithmException {
        Exception ex = null;
        try {
            String rootPath = folder.newFolder().toString();
            KVServer server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            Thread.sleep(100);
            server.shutDown();
        } catch (Exception e) {
            e.printStackTrace();
            ex = e;
        }

        assertNull(ex);
    }

    @Test
    public void testKVServerNotResponsible() throws Exception {
        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer4", null);
            server.start();
            server.startServing();
            server.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer1", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer4", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"),
                    new ECSNode("testServer2", "127.0.0.1", 50003,
                            "4a8a08f09d37b73795649038408b5f33"),
                    new ECSNode("testServer3", "127.0.0.1", 50004,
                            "8277e0910d750195b448797616e091ad"))));
            Thread.sleep(100);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id = connection.sendRequest("a", null,
                    KVMessage.StatusType.GET);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.NOT_RESPONSIBLE,
                    message.getStatus());

            id = connection.sendRequest("b", null,
                    KVMessage.StatusType.GET);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_ERROR, message.getStatus());

            id = connection.sendRequest("c", null,
                    KVMessage.StatusType.GET);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_ERROR, message.getStatus());
        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }
            if (server != null) {
                server.shutDown();
            }
        }
    }

    @Test
    public void testKVServerReplica() throws Exception {
        KVServer server = null;
        ServerConnection connection = null;
        KVServer server1 = null;
        ServerConnection connection1 = null;
        try {
            String rootPath = folder.newFolder().toString();
            server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.startServing();
            server.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer2", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"))));
            Thread.sleep(1000);

            server1 = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50002, "testServer2", null);
            server1.start();
            server1.startServing();
            server1.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer2", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"))));
            Thread.sleep(1000);

            connection1 = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection1.connect();

            int id = connection.sendRequest("a", null,
                    KVMessage.StatusType.GET);
            KVMessage message = connection.receiveMessage(id);
            assertNotEquals(KVMessage.StatusType.NOT_RESPONSIBLE,
                    message.getStatus());

            id = connection.sendRequest("b", null,
                    KVMessage.StatusType.GET);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_ERROR, message.getStatus());

        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }
            if (server != null) {
                server.shutDown();
            }
        }
    }

    @Test
    public void testKVServerWriteLock() throws Exception {
        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.startServing();
            server.updateMetadata(new Metadata(
                    Arrays.asList(
                            new ECSNode("testServer", "127.0.0.1", 50001))));
            Thread.sleep(100);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id = connection.sendRequest("a", "0",
                    KVMessage.StatusType.PUT);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_SUCCESS,
                    message.getStatus());

            server.lockWrite();

            id = connection.sendRequest("a", "1",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK,
                    message.getStatus());

            server.unlockWrite();

            id = connection.sendRequest("a", "2",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_UPDATE,
                    message.getStatus());
        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }
            if (server != null) {
                server.shutDown();
            }
        }
    }

    @Test
    public void testKVServerStartStop() throws Exception {
        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.updateMetadata(new Metadata(
                    Arrays.asList(
                            new ECSNode("testServer", "127.0.0.1", 50001))));
            Thread.sleep(100);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id = connection.sendRequest("a", "0",
                    KVMessage.StatusType.PUT);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.SERVER_STOPPED,
                    message.getStatus());

            server.startServing();

            id = connection.sendRequest("a", "1",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_SUCCESS,
                    message.getStatus());

            server.stopServing();

            id = connection.sendRequest("a", "2",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.SERVER_STOPPED,
                    message.getStatus());
        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }
            if (server != null) {
                server.shutDown();
            }
        }
    }

    @Test
    public void testKVServerSelfWriteLock() throws Exception {
        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.startServing();
            server.updateMetadata(new Metadata(
                    Arrays.asList(
                            new ECSNode("testServer", "127.0.0.1", 50001))));
            Thread.sleep(100);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id = connection.sendRequest("a", "0",
                    KVMessage.StatusType.PUT);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_SUCCESS,
                    message.getStatus());

            server.lockSelfWrite();

            id = connection.sendRequest("a", "1",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK,
                    message.getStatus());

            server.unlockWrite();

            id = connection.sendRequest("a", "2",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.SERVER_WRITE_LOCK,
                    message.getStatus());

            server.unlockSelfWrite();

            id = connection.sendRequest("a", "2",
                    KVMessage.StatusType.PUT);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_UPDATE,
                    message.getStatus());
        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }
            if (server != null) {
                server.shutDown();
            }
        }
    }

    @Test
    public void testReplicatorFullReplication() throws Exception {
        KVServer server1 = null;
        KVServer server4 = null;
        ServerConnection connection1 = null;
        ServerConnection connection4 = null;
        try {
            String rootPath = folder.newFolder().toString();
            server1 = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50002, "testServer1", null);
            server1.start();
            server1.startServing();
            server1.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer1", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer4", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"))));
            Thread.sleep(100);

            rootPath = folder.newFolder().toString();
            KVStorage storage = new KVStorage(rootPath,
                    new MD5PrefixKeyHashStrategy(1),
                    1024, IKVServer.CacheStrategy.LRU);
            server4 = new KVServer(
                    storage, new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer4", null);
            server4.start();
            server4.startServing();
            server4.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer1", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer4", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"))));
            Thread.sleep(100);

            connection4 = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection4.connect();

            int id = connection4.sendRequest("b", "5",
                    KVMessage.StatusType.PUT);
            KVMessage message = connection4.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_SUCCESS,
                    message.getStatus());

            connection1 = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50002);
            connection1.connect();

            id = connection1.sendRequest("b", null,
                    KVMessage.StatusType.GET);
            message = connection1.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_ERROR,
                    message.getStatus());

            Replicator replicator4 = new Replicator(new Protocol(),
                    new KVMessageSerializer(), server4,
                    storage);

            replicator4.startReplication();

            assertEquals(true, replicator4
                    .fullReplication("8277e0910d750195b448797616e091ad",
                            "92eb5ffee6ae2fec3ad71c777531578f", connection1));

            id = connection1.sendRequest("b", null,
                    KVMessage.StatusType.GET);
            message = connection1.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_SUCCESS,
                    message.getStatus());

        } finally {
            if (connection1 != null) {
                connection1.disconnect(true);
            }
            if (connection4 != null) {
                connection4.disconnect(true);
            }
            if (server1 != null) {
                server1.shutDown();
            }

            if (server4 != null) {
                server4.shutDown();
            }
        }
    }

    @Test
    public void testReplicatorIncrementalReplication() throws Exception {
        KVServer server = null;
        KVServer server4 = null;
        ServerConnection connection = null;
        ServerConnection connection4 = null;
        try {
            String rootPath = folder.newFolder().toString();
            server = new KVServer(
                    new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                            1024, IKVServer.CacheStrategy.LRU), new Protocol(),
                    new KVMessageSerializer(), 50002, "testServer1", null);
            server.start();
            server.startServing();
            server.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer1", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer4", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"))));
            Thread.sleep(100);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50002);
            connection.connect();

            int id = connection.sendRequest("b", null,
                    KVMessage.StatusType.GET);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_ERROR,
                    message.getStatus());

            Replicator replicator4 = new Replicator(new Protocol(),
                    new KVMessageSerializer(), null,
                    null);

            replicator4.startReplication();

            KVStorageDelta delta = new KVStorageDelta(0,
                    "8277e0910d750195b448797616e091ad",
                    "92eb5ffee6ae2fec3ad71c777531578f");

            delta.put("b", "5");

            assertEquals(true, replicator4
                    .incrementalReplication(delta, connection));

            id = connection.sendRequest("b", null,
                    KVMessage.StatusType.GET);
            message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.GET_SUCCESS,
                    message.getStatus());

        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }

            if (server != null) {
                server.shutDown();
            }
        }
    }

    @Test
    public void testKVStorageDeltaCorrectness() {
        KVStorageDelta delta;
        HashMap<String, KVStorageDelta.Value> map = new HashMap<>();
        map.clear();
        delta = new KVStorageDelta(0,
                "00000000000000000000000000000000",
                "00000000000000000000000000000000");
        delta.put("a", "1");
        delta.put("b", "2");
        delta.put("c", "3");
        delta.getEntrySet().stream()
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        assertEquals(new HashSet<String>(Arrays.asList("a", "b", "c")),
                new HashSet<>(map.keySet()));
        assertEquals("1", map.get("a").get());
        assertEquals("2", map.get("b").get());
        assertEquals("3", map.get("c").get());
    }

    @Test
    public void testKVStorageDeltaEntryCount() {
        KVStorageDelta delta;
        HashMap<String, KVStorageDelta.Value> map = new HashMap<>();
        map.clear();
        delta = new KVStorageDelta(0,
                "00000000000000000000000000000000",
                "00000000000000000000000000000000");
        assertEquals(0, delta.getEntryCount());
        delta.put("a", "1");
        delta.put("b", "2");
        delta.put("c", "3");
        assertEquals(3, delta.getEntryCount());
        delta.put("b", null);
        delta.put("c", "4");
        delta.put("d", null);
        assertEquals(4, delta.getEntryCount());
    }

    @Test
    public void testKVStorageDeltaDeletion() {
        KVStorageDelta delta;
        HashMap<String, KVStorageDelta.Value> map = new HashMap<>();
        map.clear();
        delta = new KVStorageDelta(0,
                "00000000000000000000000000000000",
                "00000000000000000000000000000000");
        delta.put("a", "1");
        delta.put("b", "2");
        delta.put("c", null);
        delta.getEntrySet().stream()
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        assertEquals(new HashSet<String>(Arrays.asList("a", "b", "c")),
                new HashSet<>(map.keySet()));
        assertEquals("1", map.get("a").get());
        assertEquals("2", map.get("b").get());
        assertEquals(null, map.get("c").get());
        map.clear();
        delta.put("b", null);
        delta.put("d", "4");
        delta.getEntrySet().stream()
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        assertEquals(new HashSet<String>(Arrays.asList("a", "b", "c", "d")),
                new HashSet<>(map.keySet()));
        assertEquals("1", map.get("a").get());
        assertEquals(null, map.get("b").get());
        assertEquals(null, map.get("c").get());
        assertEquals("4", map.get("d").get());
    }

    @Test
    public void testKVStorageDeltaPutHashRange() {
        KVStorageDelta delta;
        delta = new KVStorageDelta(0,
                "4a8a08f09d37b73795649038408b5f33",
                "92eb5ffee6ae2fec3ad71c777531578f");
        delta.put("a", "1");
        delta.put("b", "2");
        delta.put("c", "3");
        assertEquals(new HashSet<String>(Arrays.asList("b")),
                delta.getEntrySet().stream().map(Map.Entry::getKey)
                        .collect(Collectors.toSet()));
        delta = new KVStorageDelta(0,
                "4a8a08f09d37b73795649038408b5f33",
                "4a8a08f09d37b73795649038408b5f33");
        delta.put("a", "1");
        delta.put("b", "2");
        delta.put("c", "3");
        assertEquals(new HashSet<String>(Arrays.asList("a", "b", "c")),
                delta.getEntrySet().stream().map(Map.Entry::getKey)
                        .collect(Collectors.toSet()));
        delta = new KVStorageDelta(0,
                "92eb5ffee6ae2fec3ad71c777531578f",
                "4a8a08f09d37b73795649038408b5f33");
        delta.put("a", "1");
        delta.put("b", "2");
        delta.put("c", "3");
        assertEquals(new HashSet<String>(Arrays.asList("a", "c")),
                delta.getEntrySet().stream().map(Map.Entry::getKey)
                        .collect(Collectors.toSet()));
    }

    @Test
    public void testKVStorageDeltaRecording() throws NoSuchAlgorithmException,
            IOException {
        String rootDir = folder.newFolder().toString();
        KVStorage storage = new KVStorage(rootDir,
                new MD5PrefixKeyHashStrategy(1), 100,
                IKVServer.CacheStrategy.LRU);

        HashMap<String, KVStorageDelta.Value> map = new HashMap<>();
        KVStorageDelta delta;
        delta = storage
                .startNextDeltaRecording(0, "4a8a08f09d37b73795649038408b5f33",
                        "4a8a08f09d37b73795649038408b5f33");
        assertEquals(null, delta);
        storage.put("a", "1");
        storage.put("b", "2");
        storage.put("c", "3");
        delta = storage
                .startNextDeltaRecording(1, "4a8a08f09d37b73795649038408b5f33",
                        "4a8a08f09d37b73795649038408b5f33");
        map.clear();
        delta.getEntrySet().stream()
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        assertEquals(new HashSet<String>(Arrays.asList("a", "b", "c")),
                new HashSet<>(map.keySet()));
        assertEquals("1", map.get("a").get());
        assertEquals("2", map.get("b").get());
        assertEquals("3", map.get("c").get());
        storage.put("b", "3");
        storage.put("c", null);
        delta = storage
                .startNextDeltaRecording(2, "4a8a08f09d37b73795649038408b5f33",
                        "92eb5ffee6ae2fec3ad71c777531578f");
        map.clear();
        delta.getEntrySet().stream()
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        assertEquals(new HashSet<String>(Arrays.asList("b", "c")),
                new HashSet<>(map.keySet()));
        assertEquals("3", map.get("b").get());
        assertEquals(null, map.get("c").get());
        storage.put("a", "0");
        storage.put("b", "4");
        delta = storage
                .startNextDeltaRecording(3, "4a8a08f09d37b73795649038408b5f33",
                        "92eb5ffee6ae2fec3ad71c777531578f");
        map.clear();
        delta.getEntrySet().stream()
                .forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        assertEquals(new HashSet<String>(Arrays.asList("b")),
                new HashSet<>(map.keySet()));
        assertEquals("4", map.get("b").get());
        assertEquals((Integer) 3, storage.getCurrentDeltaLogicalTime());
    }
}

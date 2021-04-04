package testing;

import app_kvClient.KVClient;
import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import client.KVStore;
import client.ServerConnection;
import ecs.ECSController;
import org.junit.After;
import org.junit.Before;
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

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotSame;


public class ClientConnectionTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testTransactionBegin() throws IOException,
            InterruptedException {

        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            KVStorage storage = new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                    1024, IKVServer.CacheStrategy.LRU);
            server = new KVServer(storage, new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.startServing();
            Thread.sleep(1000);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id = connection.sendRequest(null, null,
                    KVMessage.StatusType.TRANSACTION_BEGIN);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.TRANSACTION_SUCCESS, message.getStatus());

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
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
    public void testInvalidTransactionPutGet() throws IOException,
            InterruptedException {

        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            KVStorage storage = new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                    1024, IKVServer.CacheStrategy.LRU);
            server = new KVServer(storage, new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.startServing();
            Thread.sleep(1000);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id = connection.sendRequest("a", "1",
                    KVMessage.StatusType.TRANSACTION_PUT);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.FAILED, message.getStatus());
            int id1 = connection.sendRequest("a", null,
                    KVMessage.StatusType.TRANSACTION_GET);
            KVMessage message1 = connection.receiveMessage(id1);
            assertEquals(KVMessage.StatusType.FAILED, message1.getStatus());

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
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
    public void testTransactionPutGet() throws IOException,
            InterruptedException {

        KVServer server = null;
        ServerConnection connection = null;
        try {
            String rootPath = folder.newFolder().toString();
            KVStorage storage = new KVStorage(rootPath, new MD5PrefixKeyHashStrategy(1),
                    1024, IKVServer.CacheStrategy.LRU);
            server = new KVServer(storage, new Protocol(),
                    new KVMessageSerializer(), 50001, "testServer", null);
            server.start();
            server.startServing();
            server.updateMetadata(new Metadata(Arrays.asList(
                    new ECSNode("testServer2", "127.0.0.1", 50002,
                            "0cc175b9c0f1b6a831c399e269772661"),
                    new ECSNode("testServer", "127.0.0.1", 50001,
                            "92eb5ffee6ae2fec3ad71c777531578f"))));
            Thread.sleep(1000);

            connection = new ServerConnection(new Protocol(),
                    new KVMessageSerializer(), "127.0.0.1", 50001);
            connection.connect();

            int id0 = connection.sendRequest(null, null,
                    KVMessage.StatusType.TRANSACTION_BEGIN);
            KVMessage message0 = connection.receiveMessage(id0);
            assertEquals(KVMessage.StatusType.TRANSACTION_SUCCESS, message0.getStatus());


            int id = connection.sendRequest("b", "1",
                    KVMessage.StatusType.TRANSACTION_PUT);
            KVMessage message = connection.receiveMessage(id);
            assertEquals(KVMessage.StatusType.PUT_SUCCESS, message.getStatus());
            int id1 = connection.sendRequest("b", null,
                    KVMessage.StatusType.TRANSACTION_GET);
            KVMessage message1 = connection.receiveMessage(id1);
            assertEquals(KVMessage.StatusType.GET_SUCCESS, message1.getStatus());
            assertEquals("1", message1.getValue());

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                connection.disconnect(true);
            }
            if (server != null) {
                server.shutDown();
            }
        }
    }

}

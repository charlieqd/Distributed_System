package testing;

import app_kvClient.KVClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.Assert.assertTrue;

public class ApplicationTest {

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

    public String[] runAppByCmd(String cmd) {
        inputS = new ByteArrayInputStream(cmd.getBytes());
        app = new KVClient(inputS);

        try {
            app.run();
        } catch (Exception e) {
        }
        String appMsg = outputStreamCaptor.toString().trim();
        String[] lines = appMsg.split(System.getProperty("line.separator"));

        return lines;
    }

    @Test
    public void testConnect() {
        String cmd = "connect localhost 50000\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]));
    }

    @Test
    public void testPut() {
        String cmd = "connect localhost 50000\n" + "put putTest 1\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Tuple inserted.".equals(lines[2]));
    }

    @Test
    public void testGet() {
        String cmd = "connect localhost 50000\n" + "put getTest 2\n" + "get getTest\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Tuple inserted.".equals(lines[2]) &&
                "KVClient> Value: 2".equals(lines[4]));
    }

    @Test
    public void testGetError() {
        String cmd = "connect localhost 50000\n" + "put getVal 2\n" + "get getError\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Tuple inserted.".equals(lines[2]) &&
                "KVClient> Tuple does not exist.".equals(lines[4]));
    }

    @Test
    public void testUpdate() {
        String cmd = "connect localhost 50000\n" + "put updateVal 2\n" + "put updateVal 3\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Tuple inserted.".equals(lines[2]) &&
                "KVClient> Tuple updated.".equals(lines[4]));
    }

    @Test
    public void testDelete() {
        String cmd = "connect localhost 50000\n" + "put deleteVal 2\n" + "put deleteVal\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Tuple inserted.".equals(lines[2]) &&
                "KVClient> Tuple deleted.".equals(lines[4]));
    }

    @Test
    public void testDeleteError() {
        String cmd = "connect localhost 50000\n" + "put deleteErrorVal\n" + "quit\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Tuple does not exist.".equals(lines[2]));
    }

    @Test
    public void testDisconnect() {
        String cmd = "connect localhost 50000\n" + "disconnect\n";
        String[] lines = runAppByCmd(cmd);
        assertTrue("KVClient> Connection established.".equals(lines[0]) &&
                "KVClient> Disconnecting.".equals(lines[2]));
    }

}

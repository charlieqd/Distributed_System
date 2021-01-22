package server;

import shared.Util;

import java.io.IOException;
import java.io.RandomAccessFile;

public class KVFileStorage implements IKVFileStorage {

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void put(String key, String value) {

    }

    private void saveToFile(String filePath, String key, String value) {

    }

    private String readFile(String filename, String key) throws IOException {
        RandomAccessFile writer = new RandomAccessFile(filename, "rw");
        String line = null;
        do {
            line = writer.readLine();
            String[] data = Util.csvSplitLine(line);
            if (data.size())
                if (data.get(0))
                    writer.close();
        } while (line >)
    }
}

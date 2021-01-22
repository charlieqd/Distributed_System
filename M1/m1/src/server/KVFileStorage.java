package server;

import shared.Util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class KVFileStorage implements IKVFileStorage {

    String filename;

    ReentrantLock lock;

    public KVFileStorage(String filename) {
        this.filename = filename;
        lock = new ReentrantLock();
    }

    public String read(String key) throws IOException {
        lock.lock();
        try {
            try (RandomAccessFile reader = new RandomAccessFile(filename,
                    "r")) {
                String line = reader.readLine();
                while (line != null) {
                    List<String> data = Util.csvSplitLine(line);

                    if (data.size() < 2) {
                        line = reader.readLine();
                        continue;
                    }
                    if (data.get(0).equals(key)) {
                        reader.close();
                        return data.get(1);
                    }
                    line = reader.readLine();
                }

                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public void write(String key, String value) throws
            IOException {
        lock.lock();
        try {
            try (RandomAccessFile writer = new RandomAccessFile(filename,
                    "rw")) {
                long offset = writer.getFilePointer();
                String line = writer.readLine();

                while (line != null) {

                    List<String> data = Util.csvSplitLine(line);

                    if (data.size() < 2) {
                        offset = writer.getFilePointer();
                        line = writer.readLine();
                        continue;
                    }

                    if (data.get(0).equals(key)) {

                        // if the value doesn't change, do nothing
                        if (data.get(1).equals(value)) {
                            return;
                        }
                        // read the key value pairs after target line into memory
                        int remainingLength = (int) (writer.length() - writer
                                .getFilePointer());
                        byte[] remaining = new byte[remainingLength];
                        writer.readFully(remaining);

                        // set the file pointer before the target line
                        writer.seek(offset);

                        // If value is null, delete the key value pair.
                        // If value is not null, update the key value pair
                        writeLine(key, value, writer);


                        // write back the remaining key value pairs in the file
                        writer.write(remaining);
                        writer.setLength(writer.getFilePointer());
                        return;
                    }

                    offset = writer.getFilePointer();
                    line = writer.readLine();
                }

                writeLine(key, value, writer);
            }
        } finally {
            lock.unlock();
        }
    }

    private void writeLine(String key, String value,
                           RandomAccessFile writer) throws IOException {
        if (value != null) {
            key = Util.escapeCSVString(key);
            value = Util.escapeCSVString(value);
            writer.writeBytes(key);
            writer.writeByte(',');
            writer.writeBytes(value);
            writer.writeByte('\n');
        }
    }

}
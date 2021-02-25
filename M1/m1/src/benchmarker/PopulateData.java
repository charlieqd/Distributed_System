package benchmarker;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.FileWriter;

public class PopulateData {

    private static ArrayList<String> keys = new ArrayList<>();
    public static void listFilesForFolder(final File folder) throws
            IOException {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                String filePath = fileEntry.getPath();
                System.out.println(filePath);
//                System.out.println(filePath.getBytes().length);
                String fileKey = filePath;
                if(filePath.length() > 19){
                    fileKey = filePath.substring(filePath.length() - 19);
                }
                keys.add(fileKey);
                String content = readFile(filePath, StandardCharsets.UTF_8);
                System.out.println(content);
            }
        }
    }

    static String readFile(String path, Charset encoding)
            throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public static void main(String[] args) throws IOException {
        // TODO
        int index = 1;
        String readingFilename = args[0];
        String keyFileName = args[1];
        final File folder = new File(readingFilename);
        listFilesForFolder(folder);
        System.out.println(keys.size());
        FileWriter writer = new FileWriter(keyFileName);
        for(String str: keys) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }
}
///Users/liuchenhao/Downloads/perlingiere-d/output.txt

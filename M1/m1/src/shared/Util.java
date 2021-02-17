package shared;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class Util {
    /**
     * An array mapping from a number between 0 and 15 (inclusive) to its hex
     * representation.
     */
    public static char[] hexToChar = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f'
    };

    /**
     * Make a string suitable for storage into comma-separated format, by adding
     * a backslash character before every comma, line-break, and backslash in
     * the string.
     *
     * @param s the original string.
     * @return the escaped string.
     */
    public static String escapeCSVString(String s) {
        StringBuilder sb = new StringBuilder();
        int length = s.length();
        for (int i = 0; i < length; ++i) {
            char c = s.charAt(i);
            if (c == ',' || c == '\\') {
                sb.append('\\');
                sb.append(c);
            } else if (c == '\n') {
                sb.append('\\');
                sb.append('n');
            } else if (c == '\r') {
                sb.append('\\');
                sb.append('r');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Decode a string encoded with {@link #escapeCSVString} back into its
     * original form.
     *
     * @param s the escaped string.
     * @return the original string.
     */
    public static String unescapeCSVString(String s) {
        StringBuilder sb = new StringBuilder();
        int length = s.length();
        for (int i = 0; i < length; ++i) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= length) {
                    // At the end of the string
                    break;
                }
                char next = s.charAt(i + 1);
                if (next == 'n') {
                    sb.append('\n');
                } else if (next == 'r') {
                    sb.append('\r');
                } else {
                    sb.append(next);
                }
                ++i; // Skip over the next character
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Given a line (without line ending), split the line by comma into an array
     * of strings. Each column is assumed to be the output of {@link
     * #escapeCSVString}, and will be unescaped before being returned.
     *
     * @return the list of columns.
     */
    public static List<String> csvSplitLine(String line) {
        ArrayList<String> result = new ArrayList<>();
        int length = line.length();
        int i, start = 0;
        for (i = 0; i < length; ++i) {
            char c = line.charAt(i);
            if (c == ',') {
                result.add(unescapeCSVString(line.substring(start, i)));
                start = i + 1;
            } else if (c == '\\') {
                ++i; // Skip over the next character
            }
        }
        result.add(unescapeCSVString(line.substring(start, i)));
        return result;
    }

    /**
     * Obtain the stack trace description of the given exception as a string.
     *
     * @param e the exception
     * @return the stack trace of the exception as string
     */
    public static String getStackTraceString(Exception e) {
        StringWriter s = new StringWriter();
        PrintWriter p = new PrintWriter(s);
        e.printStackTrace(p);
        return s.toString();
    }

    /**
     * Add b to a.
     */
    public static <T> void concatenateArrayLists(ArrayList<T> a,
                                                 ArrayList<T> b) {
        for (T i : b) {
            a.add(i);
        }
    }
}

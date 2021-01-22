package shared;

public class Util {
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
    public static String[] csvSplitLine(String line) {
        // TODO implement
        return new String[0];
    }
}

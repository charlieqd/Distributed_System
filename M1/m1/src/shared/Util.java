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
            if (c == ',' || c == '\\' || c == '\n') {
                sb.append('\\');
            }
            sb.append(c);
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
                continue;
            }
            sb.append(c);
        }
        return sb.toString();
    }
}

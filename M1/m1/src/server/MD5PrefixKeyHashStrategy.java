package server;

import shared.Util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5PrefixKeyHashStrategy implements KeyHashStrategy {

    private final MessageDigest hashGenerator;
    private final int numBytes;

    public MD5PrefixKeyHashStrategy(int numBytes) throws
            NoSuchAlgorithmException {
        hashGenerator = MessageDigest.getInstance("MD5");
        this.numBytes = numBytes;
    }

    @Override
    public String hashKey(String key) {
        StringBuilder sb = new StringBuilder();
        byte[] bytes = hashGenerator.digest(key.getBytes());
        int length = Math.min(bytes.length, numBytes);
        for (int i = 0; i < length; ++i) {
            byte b = bytes[i];
            sb.append(Util.hexToChar[(b >> 4) & 0x0F]);
            sb.append(Util.hexToChar[b & 0x0F]);
        }
        return sb.toString();
    }
}

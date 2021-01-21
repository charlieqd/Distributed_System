package shared;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * NOTE: Protocol methods must be thread-safe.
 */
public interface IProtocol {
    // For server

    Request readRequest(InputStream input) throws IOException;

    void writeResponse(OutputStream output,
                       Request request,
                       int status,
                       byte[] encodedMessage) throws IOException;

    // For client

    void writeRequest(OutputStream output, int id, byte[] encodedMessage)
            throws IOException;

    Response readResponse(InputStream input) throws IOException;
}

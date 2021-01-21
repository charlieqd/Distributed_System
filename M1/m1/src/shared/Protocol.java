package shared;

import java.io.*;

/**
 * TODO: implement this
 */
public class Protocol implements IProtocol {
    @Override
    public Request readRequest(InputStream input) throws IOException {
        DataInputStream stream = new DataInputStream(input);

        int id, bodySize;
        byte[] body;

        id = stream.readInt();
        bodySize = stream.readInt();
        body = new byte[bodySize];
        stream.readFully(body);

        return new Request(body, id);
    }

    @Override
    public void writeResponse(OutputStream output,
                              Request request,
                              int status,
                              byte[] encodedMessage) throws IOException {
        DataOutputStream stream = new DataOutputStream(output);

        stream.writeInt(request.getId()); // id
        stream.writeInt(status); // status
        if (encodedMessage == null) {
            stream.writeInt(0); // bodySize
        } else {
            stream.writeInt(encodedMessage.length); // bodySize
            stream.write(encodedMessage); // body
        }
        stream.flush();
    }

    @Override
    public void writeRequest(OutputStream output,
                             int id,
                             byte[] encodedMessage) throws IOException {
        DataOutputStream stream = new DataOutputStream(output);

        stream.writeInt(id); // id
        if (encodedMessage == null) {
            stream.writeInt(0); // bodySize
        } else {
            stream.writeInt(encodedMessage.length); // bodySize
            stream.write(encodedMessage); // body
        }
        stream.flush();
    }

    @Override
    public Response readResponse(InputStream input, byte[] data) throws
            IOException {
        DataInputStream stream = new DataInputStream(input);

        int id, status, bodySize;
        byte[] body;

        id = stream.readInt();
        status = stream.readInt();
        bodySize = stream.readInt();
        body = new byte[bodySize];
        stream.readFully(body);

        return new Response(body, id, status);
    }
}

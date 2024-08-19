package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

public class ByteReadFuture {
    private String _path;
    private byte[] _bytes;

    public ByteReadFuture(String path, byte[] bytes) {
        _path = path;
        _bytes = bytes;
    }

    public String getPath() {
        return _path;
    }

    public byte[] getBytes() {
        return _bytes;
    }
}

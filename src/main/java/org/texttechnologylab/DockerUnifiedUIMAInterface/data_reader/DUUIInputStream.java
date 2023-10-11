package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.ByteArrayInputStream;

public class DUUIInputStream {

    private final String _name;
    private final String _path;
    private final long _sizeBytes;
    private final ByteArrayInputStream _content;
    private byte[] _bytes;

    public DUUIInputStream(String name, String path, long sizeBytes, ByteArrayInputStream content) {
        _name = name;
        _path = path;
        _sizeBytes = sizeBytes;
        _content = content;
    }

    public String getName() {
        return _name;
    }


    public String getPath() {
        return _path;
    }


    public long getSizeBytes() {
        return _sizeBytes == 0 ? (long) getBytes().length : _sizeBytes;
    }


    public ByteArrayInputStream getContent() {
        return _content;
    }

    public byte[] getBytes() {
        if (_bytes == null) {
            _bytes = _content.readAllBytes();
        }
        return _bytes;
    }
}

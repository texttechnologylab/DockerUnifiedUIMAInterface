package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.ByteArrayInputStream;

public class DUUIExternalFile {

    private final String _name;
    private final String _path;
    private final long _sizeBytes;
    private final ByteArrayInputStream _content;

    public DUUIExternalFile(String name, String path, long sizeBytes, ByteArrayInputStream content) {
        _name = name;
        _path = path;
        _sizeBytes = sizeBytes;
        _content = content;
    }

    public String get_name() {
        return _name;
    }


    public String get_path() {
        return _path;
    }


    public long get_sizeBytes() {
        return _sizeBytes;
    }


    public ByteArrayInputStream get_content() {
        return _content;
    }
}

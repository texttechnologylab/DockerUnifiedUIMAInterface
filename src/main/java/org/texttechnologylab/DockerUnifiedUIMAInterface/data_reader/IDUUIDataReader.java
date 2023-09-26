package org.texttechnologylab.DockerUnifiedUIMAInterface.data_reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public interface IDUUIDataReader {

    AtomicInteger readProgress = new AtomicInteger(0);

    void writeFile(DUUIInputStream stream, String target);

    void writeFiles(List<DUUIInputStream> streams, String target);

    DUUIInputStream readFile(String path);

    /**
     * Special method (only) used by a DUUILocalDataReader because it is supplied with an InputStream
     * rather than a path. This method simply returns the DUUIInputStreams it received.
     *
     * @param stream The file contents in as InputStreams
     * @return A DUUIInputStream object wrapping the content read.
     */
    default DUUIInputStream readFile(DUUIInputStream stream) {
        return stream;
    }

    List<DUUIInputStream> readFiles(List<String> paths) throws IOException;

    /**
     * Special method (only) used by a DUUILocalDataReader because it is supplied with InputStreams
     * rather than paths. This method returns the DUUIInputStreams it received filtered by the specified
     * file extension.
     *
     * @param streams       The file contents in as InputStreams
     * @param fileExtension The allowed file extension.
     * @return A list of DUUIInputStream objects wrapping the content read.
     */
    default List<DUUIInputStream> readFiles(List<DUUIInputStream> streams, String fileExtension) {
        return streams.stream().filter((stream -> stream.getName().endsWith(fileExtension))).collect(Collectors.toList());
    }

    default List<String> listFiles(String folder) throws IOException {
        return listFiles(folder, "", false);
    }

    default List<String> listFiles(String folder, String fileExtension) throws IOException {
        return listFiles(folder, fileExtension, false);
    }

    List<String> listFiles(String folder, String fileExtension, boolean recursive) throws IOException;
}

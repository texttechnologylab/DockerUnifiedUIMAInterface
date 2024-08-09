package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DUUILocalDocumentHandler implements IDUUIDocumentHandler{

    @Override
    public void writeDocument(DUUIDocument document, String path) throws IOException {
        File file = new File(Paths.get(path, document.getName()).toString());
        File parent = new File(path);

        if (!parent.exists()) {
            boolean ignored = parent.mkdirs();
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(file.getAbsolutePath())) {
            fileOutputStream.write(document.getBytes());
        }
    }

    @Override
    public void writeDocuments(List<DUUIDocument> documents, String path) throws IOException {
        for (DUUIDocument document : documents) {
            writeDocument(document, path);
        }
    }

    @Override
    public DUUIDocument readDocument(String path) throws IOException {
        Path _path = Paths.get(path);
        return new DUUIDocument(
            _path.getFileName().toString(),
            _path.toFile().getAbsolutePath().replace("\\", "/"),
            Files.readAllBytes(_path)
        );
    }

    @Override
    public List<DUUIDocument> readDocuments(List<String> paths) throws IOException {
        List<DUUIDocument> documents = new ArrayList<>();

        for (String path : paths) {
            documents.add(readDocument(path));
        }
        return documents;
    }

    @Override
    public List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException {
        if (recursive) return listDocumentsRecursive(path, fileExtension);

        File[] files = new File(path).listFiles();
        if (files == null) return new ArrayList<>();

        return Stream
            .of(files)
            .filter(file -> !file.isDirectory() && file.getName().endsWith(fileExtension))
            .map(file -> new DUUIDocument(
                file.getName(),
                file.getAbsolutePath().replace("\\", "/"),
                file.length()))
            .collect(Collectors.toList());
    }

    private List<DUUIDocument> listDocumentsRecursive(String path, String fileExtension) throws IOException {
        try (Stream<Path> stream = Files.walk(Paths.get(path))) {
            return stream
                .filter(Files::isRegularFile)
                .map(file -> new DUUIDocument(
                    file.getFileName().toString(),

                    // This is only important if DUUI is run on Windows because Windows uses '\' as File.separator.
                    file.toFile().getAbsolutePath().replace("\\", "/"),
                    file.toFile().length())
                ).filter(document -> document.getName().endsWith(fileExtension))
                .collect(Collectors.toList());
        }
    }
}

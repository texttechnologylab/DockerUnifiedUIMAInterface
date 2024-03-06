package org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler;

import java.io.IOException;
import java.util.List;

public interface IDUUIDocumentHandler {

    /**
     * Write the document to the specified path.
     *
     * @param document The document to be written.
     * @param path     The full path to the destination where the document should be written.
     * @author Cedric Borkowski
     */
    void writeDocument(DUUIDocument document, String path) throws IOException;

    /**
     * Write a Collection of documents to the specified path.
     *
     * @param documents The documents to be written.
     * @param path      The full path to the destination where the documents should be written.
     * @author Cedric Borkowski
     */
    void writeDocuments(List<DUUIDocument> documents, String path) throws IOException;

    /**
     * Read the content from the specified path and return a document.
     *
     * @param path The full paths to the document that should be read.
     * @author Cedric Borkowski
     */
    DUUIDocument readDocument(String path) throws IOException;

    /**
     * Read the content from all given paths and return a List of documents.
     *
     * @param paths A list of full paths to the documents that should be read.
     * @author Cedric Borkowski
     */
    List<DUUIDocument> readDocuments(List<String> paths) throws IOException;

    /**
     * Retrieve a List of documents containing only metadata like name, path and size.
     *
     * @param path          The full path to the folder to serach for matching documents.
     * @param fileExtension Specify a fileExtension to filter the list.
     * @author Cedric Borkowski
     */
    default List<DUUIDocument> listDocuments(String path, String fileExtension) throws IOException {
        return listDocuments(path, fileExtension, false);
    }

    /**
     * Retrieve a List of documents containing only metadata like name, path and size.
     *
     * @param path          The full path to the folder to start the serach for matching documents.
     * @param fileExtension Specify a fileExtension to filter the list.
     * @param recursive     Wether the search for files with the given extension should be recursive.
     * @author Cedric Borkowski
     */
    List<DUUIDocument> listDocuments(String path, String fileExtension, boolean recursive) throws IOException;

}
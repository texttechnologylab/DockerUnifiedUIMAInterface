import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDropboxDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUILocalDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIMinioDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.writer.DocumentWriter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.mongodb.DUUIMongoDBStorageBackend;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;


public class TestMerge {

    @Test
    public void TestAddCas() throws Exception {
        DUUIComposer composer = new DUUIComposer()
            .withSkipVerification(true)
            .withDebugLevel(DUUIComposer.DebugLevel.DEBUG)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIDropboxDocumentHandler inputHandler = new DUUIDropboxDocumentHandler(
            new DbxRequestConfig("DUUI"),
            new DbxCredential(
                System.getenv("dbx_personal_access_token"),
                1L,
                System.getenv("dbx_personal_refresh_token"),
                System.getenv("dbx_app_key"),
                System.getenv("dbx_app_secret"))
        );

        DUUIMinioDocumentHandler outputHandler = new DUUIMinioDocumentHandler(
            "http://192.168.2.122:9000",
            System.getenv("minio_key"),
            System.getenv("minio_secret")
        );

        String outputPath = "output-cas";
        String outputFileExtension = ".xmi";

        DUUIDocumentReader documentReader = DUUIDocumentReader
            .builder(composer)
            .withInputHandler(inputHandler)
            .withInputPath("/input")
            .withInputFileExtension(".txt")
            .withOutputHandler(outputHandler)
            .withOutputPath(outputPath)
            .withOutputFileExtension(outputFileExtension)
            .withSortBySize(true)
            .withLanguage("de")
            .withRecursive(true)
            .withAddMetadata(true)
            .withCheckTarget(true)
            .build();

        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class)));
        composer.run(documentReader, "test");
        composer.shutdown();

        composer
            .getDocuments()
            .forEach(document -> {
                if (!document.getFileExtension().equals(outputFileExtension)) {
                    document.setOutputName(document
                        .getName()
                        .replace(document.getFileExtension(), outputFileExtension));
                }
            });

        outputHandler
            .writeDocuments(
                new ArrayList<>(composer.getDocuments())
                , outputPath);
    }

    @Test
    public void TestDocumentReader() throws Exception {

        DUUIDropboxDocumentHandler inputHandler = new DUUIDropboxDocumentHandler(
            new DbxRequestConfig("DUUI"),
            new DbxCredential(
                System.getenv("dbx_personal_access_token"),
                1L,
                System.getenv("dbx_personal_refresh_token"),
                System.getenv("dbx_app_key"),
                System.getenv("dbx_app_secret"))
        );

        DUUIMinioDocumentHandler outputHandler = new DUUIMinioDocumentHandler(
            "http://192.168.2.122:9000",
            System.getenv("minio_key"),
            System.getenv("minio_secret")
        );


        String mongoUser = System.getenv("mongo_user");
        String mongoPass = System.getenv("mongo_pass");


        DUUIComposer composer = new DUUIComposer()
            .withSkipVerification(true)
            .withDebugLevel(DUUIComposer.DebugLevel.DEBUG)
            .withWorkers(5)
            .withLuaContext(new DUUILuaContext().withJsonLibrary())
            .withStorageBackend(new DUUIMongoDBStorageBackend(
                "mongodb+srv://<user>:<pass>@testcluster.727ylpr.mongodb.net/"
                    .replace("<user>", mongoUser).replace("<pass>", mongoPass)
            ));

        String outputPath = "output-cas-2";
        String outputFileExtension = ".xmi";

        DUUIDocumentReader documentReader = DUUIDocumentReader
            .builder(composer)
            .withInputHandler(inputHandler)
            .withInputPath("/input")
            .withInputFileExtension(".txt")
            .withOutputHandler(outputHandler)
            .withOutputPath(outputPath)
            .withOutputFileExtension(outputFileExtension)
            .withSortBySize(true)
            .withLanguage("de")
            .withRecursive(true)
            .withAddMetadata(true)
            .build();

        composer.addDriver(new DUUIUIMADriver());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class))
            .withName("Tokenizer"));


        Path path = Paths.get("temp/duui/", outputPath);

        composer.add(new DUUIUIMADriver.Component(
            createEngineDescription(
                XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, path.toString(),
                XmiWriter.PARAM_STRIP_EXTENSION, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_FILENAME_EXTENSION, outputFileExtension
            )).withName("XMIWriter"));

        composer.run(documentReader, "Test");

        try {
            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            List<DUUIDocument> documents = localDocumentHandler.listDocuments(
                String.valueOf(path), "", true);

            outputHandler.writeDocuments(
                new DUUILocalDocumentHandler()
                    .readDocuments(documents
                        .stream()
                        .map(DUUIDocument::getPath)
                        .collect(Collectors.toList())), outputPath);

            File directory = new File(path.toString());
            boolean ignored = deleteTempOutputDirectory(directory);
        } catch (NoSuchFileException ignored) {
        }
    }

    @Test
    public void TestDocumentWriter() throws Exception {
        DUUIComposer composer = new DUUIComposer()
            .withSkipVerification(true)
            .withDebugLevel(DUUIComposer.DebugLevel.DEBUG)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIUIMADriver());

        DUUIDropboxDocumentHandler inputHandler = new DUUIDropboxDocumentHandler(
            new DbxRequestConfig("DUUI"),
            new DbxCredential(
                System.getenv("dbx_personal_access_token"),
                1L,
                System.getenv("dbx_personal_refresh_token"),
                System.getenv("dbx_app_key"),
                System.getenv("dbx_app_secret"))
        );

        DUUIDocumentReader documentReader = DUUIDocumentReader
            .builder(composer)
            .withInputHandler(inputHandler)
            .withInputPath("/input")
            .withInputFileExtension(".txt")
            .withLanguage("de")
            .withRecursive(true)
            .withAddMetadata(true)
            .build();

        composer.add(new DUUIUIMADriver.Component(
            createEngineDescription(
                BreakIteratorSegmenter.class
            )).withName("Tokenizer"));

        composer.add(new DUUIUIMADriver.Component(
            createEngineDescription(
                DocumentWriter.class,
                DocumentWriter.PARAM_TARGET_LOCATION, "/output/writer",
                DocumentWriter.PARAM_STRIP_EXTENSION, true,
                DocumentWriter.PARAM_OVERWRITE, true,
                DocumentWriter.PARAM_VERSION, "1.1",
                DocumentWriter.PARAM_FILENAME_EXTENSION, ".xmi",
                DocumentWriter.PARAM_PROVIDER, "Dropbox"
            )).withName("DocumentWriter"));

        composer.run(documentReader, "Test");
        // This works well when used with predefined credentials but passing them to the AE is not
        // easily realisable...
    }

    private boolean deleteTempOutputDirectory(File directory) {
        if (directory.getName().isEmpty()) {
            boolean ignored = directory.delete();
            return false;
        }

        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteTempOutputDirectory(file);
            }
        }
        return directory.delete();
    }
}

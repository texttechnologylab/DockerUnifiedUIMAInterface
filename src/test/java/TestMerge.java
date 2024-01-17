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
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.mongodb.DUUIMongoDBStorageBackend;

import java.io.File;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;


public class TestMerge {

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

        String outputPath = "output-java-new";
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
            .withMinimumDocumentSize(36000)
            .withRecursive(true)
            .withAddMetadata(true)
            .build();

        composer.addDriver(new DUUIUIMADriver());
        composer.addDriver(new DUUIRemoteDriver());
        composer.addDriver(new DUUIDockerDriver());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class))
            .withName("Tokenizer"));

        composer.add(new DUUIRemoteDriver.Component("http://192.168.2.122:9002")
            .withName("Standord POS (German)"));


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

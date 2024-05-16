import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import org.dkpro.core.tokit.ParagraphSplitter;
import org.dkpro.core.tokit.BreakIteratorSegmenter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDropboxDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIMinioDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIDocumentReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.CountAnnotations;

import java.util.Vector;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;


public class TestMerge {

    @Test
    public void TestReusablePipeline() throws Exception {
        DUUIComposer composer = new DUUIComposer()
            .withSkipVerification(true)
            .withDebugLevel(DUUIComposer.DebugLevel.DEBUG)
            .withLuaContext(new DUUILuaContext().withJsonLibrary());

        composer.addDriver(new DUUIUIMADriver());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class)).withName("BIS"));
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(ParagraphSplitter.class)).withName("Paragraph Splitter"));
        composer.instantiate_pipeline();
        Vector<DUUIComposer.PipelinePart> pipeline = composer.getInstantiatedPipeline();

        DUUIDropboxDocumentHandler inputHandler = new DUUIDropboxDocumentHandler(
            new DbxRequestConfig("DUUI"),
            new DbxCredential(
                System.getenv("dbx_personal_access_token"),
                1L,
                System.getenv("dbx_personal_refresh_token"),
                System.getenv("dbx_app_key"),
                System.getenv("dbx_app_secret"))
        );

        DUUIDocumentReader documentReader1 = DUUIDocumentReader
            .builder(composer)
            .withInputHandler(inputHandler)
            .withInputPath("/input")
            .withInputFileExtension(".txt")
            .withOutputHandler(inputHandler)
            .withOutputPath("/output/1")
            .withOutputFileExtension(".txt")
            .withSortBySize(true)
            .withLanguage("de")
            .withRecursive(true)
            .withAddMetadata(true)
            .withCheckTarget(true)
            .build();

        DUUIComposer composer2 = new DUUIComposer()
            .withInstantiatedPipeline(pipeline)
            .withIgnoreErrors(true)
            .withWorkers(5)
            .withDebugLevel(DUUIComposer.DebugLevel.DEBUG)
            .withLuaContext(new DUUILuaContext().withJsonLibrary())
            .withSkipVerification(true);

        composer2.asService(true).run(documentReader1, "2");
        composer2.shutdown();
    }

    @Test
    public void TestDocuments() throws Exception {
        DUUIComposer composer = new DUUIComposer()
            .withSkipVerification(true)
            .withWorkers(5)
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

        String outputPath = "output/triple-uima";
        String outputFileExtension = ".txt";

        DUUIDocumentReader documentReader = DUUIDocumentReader
            .builder(composer)
            .withInputHandler(outputHandler)
            .withInputPath("output")
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

        composer.addDriver(new DUUIDockerDriver());
        composer.addDriver(new DUUIUIMADriver());
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BreakIteratorSegmenter.class)).withName("Tokenizer"));
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(ParagraphSplitter.class)).withName("Paragraph"));
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(CountAnnotations.class)).withName("Counter"));
        composer.run(documentReader, "test");
        composer.shutdown();
    }
}

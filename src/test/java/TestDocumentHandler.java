import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.files.WriteMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDocument;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIDropboxDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUILocalDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIMinioDocumentHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.Timer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestDocumentHandler {

    private static final DbxCredential dropboxCredentials = new DbxCredential(
        System.getenv("dbx_personal_access_token"),
        1L,
        System.getenv("dbx_personal_refresh_token"),
        System.getenv("dbx_app_key"),
        System.getenv("dbx_app_secret"));

    private static final DbxRequestConfig dropboxConfig = new DbxRequestConfig("DUUI");

    @Nested
    @DisplayName("Dropbox")
    public class TestDropboxHandler {


        @Test
        public void TestListFiles() throws IOException, DbxException {
            DUUIDropboxDocumentHandler dropboxDocumentHandler = new DUUIDropboxDocumentHandler(
                dropboxConfig,
                dropboxCredentials
            );

            assertEquals(
                1,
                dropboxDocumentHandler.listDocuments("/input", ".gz", false).size());

            assertEquals(
                34,
                dropboxDocumentHandler.listDocuments("/input", ".gz", true).size());

            assertEquals(
                20,
                dropboxDocumentHandler.listDocuments("/input", ".txt", true).size());

            assertEquals(
                0,
                dropboxDocumentHandler.listDocuments("/input", ".txt", false).size());
        }

        @Test
        public void TestWriteFile() throws IOException, DbxException {
            DUUIDropboxDocumentHandler dropboxDocumentHandler = new DUUIDropboxDocumentHandler(
                dropboxConfig,
                dropboxCredentials
            );

            String text = "print(\"Hello World!\")";
            String outputPath = "/output/java/py";

            DUUIDocument document = new DUUIDocument(
                "main.py",
                outputPath,
                text.getBytes(StandardCharsets.UTF_8)
            );

            dropboxDocumentHandler.writeDocument(document, document.getPath());

            text = "print(\"Hello World!\");";

            document = new DUUIDocument(
                "main.py",
                outputPath,
                text.getBytes(StandardCharsets.UTF_8)
            );

            dropboxDocumentHandler.setWriteMode(WriteMode.OVERWRITE);
            dropboxDocumentHandler.writeDocument(document, document.getPath());

            assertEquals(1, dropboxDocumentHandler.listDocuments(outputPath, ".py").size());
        }

        @Test
        public void TestWriteFiles() throws DbxException, IOException {
            DUUIDropboxDocumentHandler dropboxDocumentHandler = new DUUIDropboxDocumentHandler(
                dropboxConfig,
                dropboxCredentials
            );

            String outputPath = "/output/java/txt";

            List<DUUIDocument> documents = dropboxDocumentHandler
                .readDocuments(dropboxDocumentHandler
                    .listDocuments("/input", ".txt", true)
                    .stream()
                    .map(DUUIDocument::getPath)
                    .collect(Collectors.toList()));

            dropboxDocumentHandler.setWriteMode(WriteMode.ADD);
            dropboxDocumentHandler.writeDocuments(documents, outputPath);

            assertEquals(20, dropboxDocumentHandler.listDocuments(outputPath, ".txt").size());
        }

        @Test
        public void TestWriteLargeFile() throws IOException, DbxException {
            DUUIDropboxDocumentHandler dropboxDocumentHandler = new DUUIDropboxDocumentHandler(
                dropboxConfig,
                dropboxCredentials
            );

            String path = "D:\\Uni Informatik B.sc\\Bachelor\\DockerUnifiedUIMAInterface-Fork\\src\\main\\resources\\sample\\18011.xmi.gz.xmi.gz";

            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            DUUIDocument document = localDocumentHandler.readDocument(path);

            String outputPath = "/output/large";
            Timer timer = new Timer();
            timer.start();
            dropboxDocumentHandler.writeDocument(document, outputPath);
            timer.stop();
            System.out.println(timer.getDuration() / 1000 + " s");

            assertEquals(1, dropboxDocumentHandler.listDocuments(outputPath, ".gz").size());
        }

        @Test
        public void TestReadFile() throws IOException, DbxException {
            DUUIDropboxDocumentHandler dropboxDocumentHandler = new DUUIDropboxDocumentHandler(
                dropboxConfig,
                dropboxCredentials
            );

            String expected = "print(\"Hello World!\");";
            DUUIDocument document = dropboxDocumentHandler.readDocument("/output/java/py/main.py");

            assertEquals(expected, document.getText().trim());
        }

        @Test
        public void TestReadFiles() throws DbxException, IOException {
            DUUIDropboxDocumentHandler dropboxDocumentHandler = new DUUIDropboxDocumentHandler(
                dropboxConfig,
                dropboxCredentials
            );

            List<DUUIDocument> input = dropboxDocumentHandler
                .readDocuments(
                    dropboxDocumentHandler
                        .listDocuments("/output/java/txt", ".txt")
                        .stream()
                        .map(DUUIDocument::getPath)
                        .collect(Collectors.toList()));

            List<DUUIDocument> output = dropboxDocumentHandler
                .readDocuments(
                    dropboxDocumentHandler
                        .listDocuments("/input", ".txt", true)
                        .stream()
                        .map(DUUIDocument::getPath)
                        .collect(Collectors.toList()));

            assertEquals(input.size(), output.size());
        }
    }

    @Nested
    @DisplayName("min.io")
    public class TestMinioDocumentHandler {

        private final String endpoint = "http://192.168.2.122:9000";

        @Test
        public void TestListFiles() throws IOException {
            DUUIMinioDocumentHandler reader = new DUUIMinioDocumentHandler(
                endpoint,
                System.getenv("minio_key"),
                System.getenv("minio_secret"));

            assertEquals(1, reader.listDocuments("output/path", ".txt", true).size());
            assertEquals(1, reader.listDocuments("output/python", ".py", true).size());
            assertEquals(20, reader.listDocuments("output/path", ".xmi", true).size());
            assertEquals(20, reader.listDocuments("output/path/to/file", ".xmi").size());
        }

        @Test
        public void TestMinioWriteFile() throws IOException {
            DUUIMinioDocumentHandler _reader = new DUUIMinioDocumentHandler(
                endpoint,
                System.getenv("minio_key"),
                System.getenv("minio_secret"));

            String text = "print(\"Hello World!\")";
            String name = "main.py";
            String path = "output/python";

            DUUIDocument document = new DUUIDocument(
                name,
                path,
                text.trim().getBytes(StandardCharsets.UTF_8)
            );

            _reader.writeDocument(document, path);
            assertEquals(1, _reader.listDocuments(path, ".py").size());
        }

        @Test
        public void TestWriteLargeFile() throws IOException {
            DUUIMinioDocumentHandler _reader = new DUUIMinioDocumentHandler(
                endpoint,
                System.getenv("minio_key"),
                System.getenv("minio_secret"));

            String path = "D:\\Uni Informatik B.sc\\Bachelor\\DockerUnifiedUIMAInterface-Fork\\src\\main\\resources\\sample\\18011.xmi.gz.xmi.gz";

            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            DUUIDocument document = localDocumentHandler.readDocument(path);

            _reader.writeDocument(document, "output/large");
            assertEquals(1, _reader.listDocuments("output/large", ".gz").size());
        }

        @Test
        public void TestMinioReadFile() throws IOException {
            DUUIMinioDocumentHandler _reader = new DUUIMinioDocumentHandler(
                endpoint,
                System.getenv("minio_key"),
                System.getenv("minio_secret"));

            String expected = "print(\"Hello World!\")";


            DUUIDocument document = _reader.readDocument("output/python/main.py");
            String content = new String(document.getBytes(), StandardCharsets.UTF_8);
            assertEquals(expected.trim(), content.trim());
        }


    }

    @Nested
    @DisplayName("Local")
    public class TestLocalDocumentHandler {

        @Test
        public void TestListFiles() throws IOException {
            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            assertEquals(3,
                localDocumentHandler.listDocuments(
                    "N:/duui/tests/input/test_corpora_xmi",
                    ".xmi").size()
            );
        }

        @Test
        public void TestReadFile() throws IOException {
            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            String expectedText = "Hello World!";

            DUUIDocument document = localDocumentHandler.readDocument("N:/duui/tests/input/test_corpora/1.txt");
            assertEquals(expectedText.trim(), document.getText().trim());
        }

        @Test
        public void TestReadFiles() throws IOException {
            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            List<String> paths = localDocumentHandler
                .listDocuments("N:/duui/tests/input/test_corpora", ".txt")
                .stream()
                .map(DUUIDocument::getPath)
                .collect(Collectors.toList());
            List<DUUIDocument> documents = localDocumentHandler.readDocuments(paths);
            String[] texts = new String[]{
                "Hello World!",
                "Simple pipelines.",
                "This is also a very small text."
            };

            assertEquals(3, documents.size());
            for (int i = 0; i < texts.length; i++) {
                assertEquals(texts[i].trim(), documents.get(i).getText().trim());
            }
        }

        @Test
        public void TestWriteFile() throws IOException {
            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();

            String text = "Das ist ein Test.";
            String name = "test.txt";
            String path = "N:/duui/tests/input/test.txt";
            String target = "N:/duui/tests/output";

            DUUIDocument document = new DUUIDocument(
                name,
                path,
                text.trim().getBytes(StandardCharsets.UTF_8)
            );
            localDocumentHandler.writeDocument(document, target);

            assertEquals(1, localDocumentHandler.listDocuments(target, ".txt").size());
            assertEquals(0, localDocumentHandler.listDocuments(target, ".py").size());
        }

        @Test
        public void TestWriteFiles() throws IOException {

            List<DUUIDocument> documents = new ArrayList<>(List.of(
                new DUUIDocument("hello.py", "N:/duui/tests/input/hello.py", "print(\"Hello\")".trim().getBytes(StandardCharsets.UTF_8)),
                new DUUIDocument("main.py", "N:/duui/tests/input/main.py", "print(\"main\")".trim().getBytes(StandardCharsets.UTF_8)),
                new DUUIDocument("bye.py", "N:/duui/tests/input/bye.py", "print(\"bye\")".trim().getBytes(StandardCharsets.UTF_8))
            ));

            String outputPath = "N:/duui/tests/output/python";
            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            localDocumentHandler.writeDocuments(documents, outputPath);

            assertEquals(3, localDocumentHandler.listDocuments(outputPath, ".py").size());
        }

        @Test
        public void TestWriteLargeFile() throws IOException {
            String path = "D:\\Uni Informatik B.sc\\Bachelor\\DockerUnifiedUIMAInterface-Fork\\src\\main\\resources\\sample\\18011.xmi.gz.xmi.gz";

            DUUILocalDocumentHandler localDocumentHandler = new DUUILocalDocumentHandler();
            DUUIDocument document = localDocumentHandler.readDocument(path);

            String outputPath = "N:/duui/tests/output/large_file";
            Timer timer = new Timer();
            timer.start();
            localDocumentHandler.writeDocument(document, outputPath);
            timer.stop();
            System.out.println(timer.getDuration() + " ms");
            assertEquals(1, localDocumentHandler.listDocuments(outputPath, ".gz").size());
        }
    }

}
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReaderLazy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIParallelFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;


public class Corpora {


    @Test
    public void readingTest() throws IOException {

        String sourcePath = "/storage/projects/Verben/c4";

        long lStart = System.currentTimeMillis();
//        List<Path> pathList = Files.walk(Path.of(sourcePath)).filter(f->{
//            return f.getFileName().endsWith(".xmi.gz");
//        }).collect(Collectors.toList());

        AtomicInteger pCounter = new AtomicInteger(0);

//        ConcurrentLinkedQueue<Path> pList = new ConcurrentLinkedQueue<>();
        List<String> pList = Collections.synchronizedList(new ArrayList<>());

        DUUIParallelFileReader pReader = new DUUIParallelFileReader(new File(sourcePath), "xmi.gz", pList, 3);

        System.out.println(pList.size());


        //getFiles(Path.of(sourcePath), pList);


    }


    @Test
    public void C4() throws Exception {

        int iScale = 20;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        composer.addDriver(uimaDriver, dockerDriver);

        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy:0.3.0")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(spacy.withSegmentationStrategy(pStrategy));

//        composer.add(component);

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/corpora/c4/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());

        String sourcePath = "/storage/projects/Verben/c4";

        DUUIFileReaderLazy dFileReader = new DUUIFileReaderLazy(sourcePath, "xmi.gz");

        DUUIAsynchronousProcessor collectionReader = new DUUIAsynchronousProcessor(dFileReader);

//        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, ".xmi.gz", 1, false);

        composer.run(collectionReader, "spacy");

    }

    @Test
    public void Twitter() throws Exception {

        int iScale = 20;

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(iScale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        composer.addDriver(uimaDriver, dockerDriver);

        DUUIDockerDriver.Component spacy = new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy:0.3.0")
                .withScale(iScale).withImageFetching();

        DUUISegmentationStrategy pStrategy = new DUUISegmentationStrategyByDelemiter()
                .withDelemiter(".")
                .withLength(100000)
                .withDebug()
                .withOverlap(500);

        composer.add(spacy.withSegmentationStrategy(pStrategy));

//        composer.add(component);

        AnalysisEngineDescription writerEngineCAS = createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, "/tmp/corpora/twitter_sample/",
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        );

//        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());
        composer.add(new DUUIUIMADriver.Component(writerEngineCAS).build());

        String sourcePath = "/storage/projects/Verben/twitter_sample/xmi";

        DUUIFileReaderLazy dFileReader = new DUUIFileReaderLazy(sourcePath, "xmi.gz");

        DUUIAsynchronousProcessor collectionReader = new DUUIAsynchronousProcessor(dFileReader);


//        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, ".xmi.gz", 1, false);

        composer.run(collectionReader, "spacy");

    }

}

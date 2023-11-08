import com.google.common.io.Files;
import de.tudarmstadt.ukp.dkpro.core.api.lexmorph.type.pos.POS;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import de.tudarmstadt.ukp.dkpro.core.api.ner.type.NamedEntity;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token;
import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasIOUtils;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.CountAnnotations;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.RemoveAnnotations;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.RemoveMetaData;
import org.texttechnologylab.utilities.helper.ArchiveUtils;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class ExperimentLecture {

    @Test
    public void GerParCorClean() throws Exception {

        String sInputPath = "/storage/xmi/GerParCorDownload/";
        String sOutputPath = "/tmp/cleanDUUI/";
        String sSuffix = "xmi.gz";

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 1, 10, false, "/tmp/sampleDUUI");

        int iWorkers = Integer.valueOf(1);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveAnnotations.class)).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "clean");
    }



    @Test
    public void GerParCorToken() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/0_plain";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/0_token";
        String sSuffix = "xmi.gz";
        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);


        DUUIDockerDriver  docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withImageFetching().withScale(iWorkers).build());
        composer.add(new DUUIDockerDriver.Component("duui-spacy-tokenizer:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "token");


    }

    @Test
    public void GerParCorSentence() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/0_token";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sSuffix = "xmi.gz";

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);
        new File(sOutputPath).mkdir();

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);


        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("sentence", composer);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withImageFetching().withScale(iWorkers).build());
        composer.add(new DUUIDockerDriver.Component("duui_simple_sentence:0.1").withScale(iWorkers).build());
//                .withParameter("split_large_texts", "tsrue")
//                .withScale(iWorkers).build());
//        composer.add(new DUUIRemoteDriver.Component("http://localhost:9714")
//                .withParameter("split_large_texts", "True")
//                .withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "sentence");


    }

    @Test
    public void PraktikumExampleSatz() throws Exception {

        // Input- und Output-Pfade
        String sInputPath = "/storage/xmi/GerParCorDownload";
        String sOutputPath = "/tmp/output";
        String sSuffix = "xmi.gz";

        // Asynchroner reader für die Input-Dateien
        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 1, 10, true, "/tmp/example");
        new File(sOutputPath).mkdir();

        // Definition der Anzahl der Prozesse
        int iWorkers = Integer.valueOf(1);

        // Lua-Kontext für die Nutzung von Lua
        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();

        // Instanziierung des Composers, mit einigen Parametern
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)     // wir überspringen die Verifikation aller Componenten =)
                .withLuaContext(ctx)            // wir setzen den definierten Kontext
                .withWorkers(iWorkers);         // wir geben dem Composer eine Anzahl an Threads mit.


        /**
         * Definition verschiedener Driver
         */
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver().withSwarmVisualizer(8888);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        // Hinzufügen der einzelnen Driver zum Composer
        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

        // Hinzfügen einer Componente in den Docker-Driver; Skalierung wie im Composer; Achtung: Image ist nur lokal verfügbar, Namensraum beachten!
//        composer.add(new DUUIDockerDriver.Component("duui_simple_sentence:0.1").withScale(iWorkers).build());

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withImageFetching()
                .withScale(iWorkers)
                .build());

        // Hinzufügen einer UIMA-Componente zum schreiben der Ergebnisse
        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        // Starten des Composers mit dem Reader und dem Namen des Jobs
        composer.run(pCorpusReader, "sentence");

    }

    @Test
    public void GerParCorComplete() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/3_pos";
        String sSuffix = "xmi.gz";
        new File(sOutputPath).mkdir();

        String sRun = "complete";

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(1);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_segmentation.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun(sRun, composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);


        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

        // Token
        composer.add(new DUUIDockerDriver.Component("duui-spacy-tokenizer:0.1").withScale(iWorkers).build());

        // Sentence
        composer.add(new DUUIDockerDriver.Component("duui_simple_sentence:0.1").withScale(iWorkers).build());

        // POS
        List<String> sPosList = new ArrayList<>();
        sPosList.add("http://geltlin.hucompute.org:8101");
        composer.add(new DUUIRemoteDriver.Component(sPosList)).withWorkers(iWorkers);

        // LEMMA
        composer.add(new DUUIDockerDriver.
                Component("docker.texttechnologylab.org/textimager_duui_hanta:latest")
                .withScale(iWorkers));

        // NER
        List<String> sNERList = new ArrayList<>();
        sNERList.add("http://geltlin.hucompute.org:8102");
        composer.add(new DUUIRemoteDriver.Component(sNERList)).withWorkers(iWorkers);

        // DEP
        List<String> sDEPList = new ArrayList<>();
        sDEPList.add("http://geltlin.hucompute.org:8103");
        composer.add(new DUUIRemoteDriver.Component(sDEPList)).withWorkers(iWorkers);

        // SRL
        List<String> sSRLList = new ArrayList<>();
        sSRLList.add("http://geltlin.hucompute.org:8104");
        composer.add(new DUUIRemoteDriver.Component(sSRLList)).withWorkers(iWorkers);

        // TOPIC
        List<String> sTopicList = new ArrayList<>();
        sTopicList.add("http://geltlin.hucompute.org:8105");

        composer.add(new DUUIRemoteDriver.Component(sTopicList)
                        .withParameter("model_name", "chkla/parlbert-topic-german")
                        .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence"))
                .withWorkers(iWorkers);

        // Sentiment
        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.1")
                .withParameter("model_name", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
                .withScale(iWorkers));

        // CoRef
        List<String> sCoRefList = new ArrayList<>();
        sCoRefList.add("http://geltlin.hucompute.org:8106");
        composer.add(new DUUIRemoteDriver.Component(sCoRefList)).withWorkers(iWorkers);

        // Abstract
        List<String> sAbstractList = new ArrayList<>();
        sAbstractList.add("http://geltlin.hucompute.org:8107");

        composer.add(new DUUIRemoteDriver.Component(sAbstractList)
                .withParameter("model_name", "Google T5-base")
                .withParameter("summary_length", "75"))
                .withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, sRun);


    }

    @Test
    public void GerParCorPOS() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/3_pos";
        String sSuffix = "xmi.gz";
        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("pos", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());

        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8011");
        sList.add("http://geltlin.hucompute.org:8012");
        sList.add("http://geltlin.hucompute.org:8013");
        sList.add("http://geltlin.hucompute.org:8014");
        sList.add("http://geltlin.hucompute.org:8015");
//        sList.add("http://geltlin.hucompute.org:8103");
//        sList.add("http://geltlin.hucompute.org:8104");
//        sList.add("http://geltlin.hucompute.org:8105");

        composer.add(new DUUIRemoteDriver.Component(sList)).withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "pos");


    }


    @Test
    public void GerParCorNER() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/4_ner";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("ner", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());

        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8016");
        sList.add("http://geltlin.hucompute.org:8017");
        sList.add("http://geltlin.hucompute.org:8018");
        sList.add("http://geltlin.hucompute.org:8019");
        sList.add("http://geltlin.hucompute.org:8020");
//        sList.add("http://geltlin.hucompute.org:8103");
//        sList.add("http://geltlin.hucompute.org:8104");
//        sList.add("http://geltlin.hucompute.org:8105");

        composer.add(new DUUIRemoteDriver.Component(sList)).withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "ner");


    }


    @Test
    public void GerParCorDEP() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/5_dep";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("dep", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, remote_driver, docker_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());


        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8006");
        sList.add("http://geltlin.hucompute.org:8007");
        sList.add("http://geltlin.hucompute.org:8008");
        sList.add("http://geltlin.hucompute.org:8009");
        sList.add("http://geltlin.hucompute.org:8010");

        composer.add(new DUUIRemoteDriver.Component(sList)).withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "dep");


    }

    @Test
    public void GerParCorSRL() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/6_srl";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);



        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("srl", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8001");
        sList.add("http://geltlin.hucompute.org:8002");
        sList.add("http://geltlin.hucompute.org:8003");
        sList.add("http://geltlin.hucompute.org:8004");
        sList.add("http://geltlin.hucompute.org:8005");

        composer.add(new DUUIRemoteDriver.Component(sList)).withWorkers(iWorkers);
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "srl");


    }


    @Test
    public void GerParCorTOPIC() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/7_topic";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("topic_new", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver(1000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();


        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());


//        composer.add(new DUUIDockerDriver.
//                Component("docker.texttechnologylab.org/textimager-duui-transformers-topic:0.0.1")
//                .withParameter("model_name", "chkla/parlbert-topic-german")
//                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
//                .withScale(iWorkers));

        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8106");
        sList.add("http://geltlin.hucompute.org:8107");
        sList.add("http://geltlin.hucompute.org:8108");
        sList.add("http://geltlin.hucompute.org:8109");
        sList.add("http://geltlin.hucompute.org:8110");
//        sList.add("http://geltlin.hucompute.org:8103");
//        sList.add("http://geltlin.hucompute.org:8104");
//        sList.add("http://geltlin.hucompute.org:8105");

        composer.add(new DUUIRemoteDriver.Component(sList)
                .withParameter("model_name", "chkla/parlbert-topic-german")
                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence"))
                .withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveMetaData.class)).build());


        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "topic_new");


    }



    @Test
    public void GerParCorSentiment() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/8_sentiment";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 1, false);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("sentiment", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver(1000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());


        composer.add(new DUUISwarmDriver.
                Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.1")
                .withParameter("model_name", "cardiffnlp/twitter-xlm-roberta-base-sentiment")
                .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
                .withScale(iWorkers));



        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "sentiment");


    }



    @Test
    public void GerParCorLemma() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/11_lemma";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 1, false);

        int iWorkers = Integer.valueOf(10);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("lemma", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver(1000);
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());


        composer.add(new DUUISwarmDriver.
                Component("docker.texttechnologylab.org/textimager_duui_hanta:latest")
                .withScale(iWorkers));

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "lemma");


    }


    @Test
    public void countAnnotations() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/";

        File fFiles = new File(sInputPath);

        for (File file : fFiles.listFiles()) {

            if(file.isDirectory() && file.getName().contains("lemma")){

                String sSuffix = "xmi.gz";

                AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(file.getAbsolutePath(), sSuffix, -1, true);

                int iWorkers = Integer.valueOf(1);

                DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
                DUUIComposer composer = new DUUIComposer()
                        .withSkipVerification(true)
                        .withLuaContext(ctx)
                        .withWorkers(iWorkers);


                DUUIDockerDriver docker_driver = new DUUIDockerDriver();
                DUUISwarmDriver swarm_driver = new DUUISwarmDriver(1000);
                DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                        .withDebug(true);

                composer.addDriver(swarm_driver, uima_driver, docker_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/ner:latest").withImageFetching().withScale(iWorkers).build());

                composer.add(new DUUIUIMADriver.Component(createEngineDescription(CountAnnotations.class)).withScale(iWorkers).build());

                composer.run(pCorpusReader, "count");

            }

        }



    }

    @Test
    public void countSize(){


        String sInputPath = "/home/staff_homes/abrami/Lecture_2/";

        int corpusMax = 1711285;
        int corpusSample = 2000;

        File fInput = new File(sInputPath);

        for (File file : fInput.listFiles()) {

            if(file.isDirectory()){
                System.out.print(file.getName());

                try {
//                    System.out.println(FileUtils.getFiles(file.getAbsolutePath(), ".gz").size());
                    long fSize = FileUtils.getFiles(file.getAbsolutePath(), ".gz").stream().mapToLong(f->{
                        RandomAccessFile raf = null;
                        int fileSize = 0;
                        try {
                            raf = new RandomAccessFile(f, "r");
                            raf.seek(raf.length() - 4);
                            byte[] bytes = new byte[4];
                            raf.read(bytes);
                            fileSize = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
                            if (fileSize < 0)
                                fileSize += (1L << 32);
                            raf.close();
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return fileSize;

                    }).sum();
                    System.out.print("\t");

                    double newSize = (double)fSize;
                    newSize = newSize/corpusSample;
                    newSize = newSize*corpusMax;
                    System.out.print((double)fSize/1000000000+"\t"+(double)newSize/1000000000);
                    System.out.print("\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }

    }

    @Test
    public void getData() throws IOException, UIMAException {


        String sInputPath = "/tmp/result";
        JCas pCas = JCasFactory.createJCas();
        FileUtils.getFiles(sInputPath, ".xmi").forEach(f->{
            System.out.println(f.getName());
            pCas.reset();
            try {
                CasIOUtils.load(f.toURL(), pCas.getCas());

                try {
                    Sentence pSentence = JCasUtil.select(pCas, Sentence.class).stream().findFirst().get();

                    JCasUtil.selectCovered(Annotation.class, pSentence).forEach(t -> {
                        if (!(t instanceof Token) && !(t instanceof DocumentMetaData)) {

                            if (t instanceof POS) {
                                System.out.println(t.getBegin() + "\t" + t.getEnd() + "\t" + ((POS) t).getPosValue() + "\t" + t.getCoveredText());
                            }
                            else if (t instanceof NamedEntity) {
                                System.out.println(t.getBegin() + "\t" + t.getEnd() + "\t" + ((NamedEntity) t).getValue() + "\t" + t.getCoveredText());
                            }
                            else {
                                System.out.println(t.getBegin() + "\t" + t.getEnd() + "\t" + t.getCoveredText());
                            }


                        }
                    });
                }
                catch (Exception e){

                }

            } catch (IOException e) {
                e.printStackTrace();
            }




        });

    }

    @Test
    public void getXMI() {


        String sInputPath = "/home/staff_homes/abrami/Lecture_2/";

        int corpusMax = 1711285;
        int corpusSample = 2000;

        String sOutput = "/tmp/result";
        new File(sOutput).mkdir();

        File fInput = new File(sInputPath);

        for (File file : fInput.listFiles()) {

            if (file.isDirectory()) {
                File nFile = new File(sInputPath + file.getName() + "/" + "1001-SZ-08051992.tei/1001-SZ-08051992.tei#222.xmi.gz");

                try {
                    File decompressedFile = ArchiveUtils.decompressGZ(nFile);
                    Files.move(decompressedFile, new File(sOutput+"/"+file.getName()+".xmi"));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        }
    }



    @Test
    public void GerParCorCoRef() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/9_coref";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 1, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("coref", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/gercoref_cuda:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/gercoref:latest").withImageFetching().withScale(iWorkers).build());

        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8101");
        sList.add("http://geltlin.hucompute.org:8102");
        sList.add("http://geltlin.hucompute.org:8103");
        sList.add("http://geltlin.hucompute.org:8104");
        sList.add("http://geltlin.hucompute.org:8105");

        composer.add(new DUUIRemoteDriver.Component(sList)).withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(CountAnnotations.class)).withScale(1).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "coref");


    }

    @Test
    public void GerParAbstractSingle() throws Exception {

        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/12_abstract_new";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        String sContent = FileUtils.getContentFromFile(new File("/home/staff_homes/abrami/Lecture_2/bonanza.txt"));

        JCas newCas = JCasFactory.createText(sContent, "de");

//        JCas newCas = JCasFactory.createText("Sampdoria Genua, italienischer Fußball-Meister, verpflichtete für umgerechnet rund 2,5 Millionen Mark den englischen Fußball-Nationalspieler Des Walker (27) vom Erstligisten Nottingham Forest; der Verteidiger unterschrieb einen Zweijahresvertrag, der ihm ein Jahresgehalt von 835000 Dollar garantiert.", "de");

        DocumentMetaData dmd = new DocumentMetaData(newCas);
        dmd.setDocumentTitle("Bonanza");
        dmd.setDocumentId("Bonanza.xmi");
        dmd.addToIndexes();

        int iWorkers = Integer.valueOf(1);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("abstract_new", composer);

        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/gercoref_cuda:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/gercoref:latest").withImageFetching().withScale(iWorkers).build());

//        composer.add(new DUUIDockerDriver.Component("duui-spacy-tokenizer:0.1").withImageFetching().withScale(iWorkers).build());
        composer.add(new DUUIDockerDriver.Component("duui_simple_sentence:0.1").withImageFetching().withScale(iWorkers).build());


        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8101");
//        sList.add("http://geltlin.hucompute.org:8102");
//        sList.add("http://geltlin.hucompute.org:8103");
//        sList.add("http://geltlin.hucompute.org:8104");
//        sList.add("http://geltlin.hucompute.org:8105");

        composer.add(new DUUIRemoteDriver.Component(sList).withParameter("model_name", "Google T5-base").withParameter("summary_length", "75")).withWorkers(iWorkers);

//        composer.add(new DUUIUIMADriver.Component(createEngineDescription(CountAnnotations.class)).withScale(1).build());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(newCas, "abstract_new");


    }

    @Test
    public void GerParCorAbstract() throws Exception {

        String sInputPath = "/home/staff_homes/abrami/Lecture_2/2_sentence";
        String sOutputPath = "/home/staff_homes/abrami/Lecture_2/10_abstract";
        String sSuffix = "xmi.gz";

        new File(sOutputPath).mkdir();

        AsyncCollectionReader pCorpusReader = new AsyncCollectionReader(sInputPath, sSuffix, 10, true);

        int iWorkers = Integer.valueOf(5);

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("benchmark_lecture_new.db")
                .withConnectionPoolSize(iWorkers);

        DUUILuaContext ctx = new DUUILuaContext().withJsonLibrary();
        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withWorkers(iWorkers);

        sqlite.addNewRun("abstract", composer);

        DUUIRemoteDriver remote_driver = new DUUIRemoteDriver();
        DUUIDockerDriver docker_driver = new DUUIDockerDriver();
        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, docker_driver, remote_driver);

//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/udepparser_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/srl_cuda_1024:latest").withImageFetching().withScale(iWorkers).build());
//        composer.add(new DUUISwarmDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("flair_pos:0.1").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/flair/pos:latest").withScale(iWorkers).build());
//        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-transformers-summary-cuda:latest").withImageFetching().withScale(iWorkers).build());

        List<String> sList = new ArrayList<>();
        sList.add("http://geltlin.hucompute.org:8106");
        sList.add("http://geltlin.hucompute.org:8107");
        sList.add("http://geltlin.hucompute.org:8108");
        sList.add("http://geltlin.hucompute.org:8109");
        sList.add("http://geltlin.hucompute.org:8110");

        composer.add(new DUUIRemoteDriver.Component(sList).withParameter("model_name", "Google T5-base").withParameter("summary_length", "75")).withWorkers(iWorkers);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, sOutputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(pCorpusReader, "abstract");


    }


}

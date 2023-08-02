import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.uima.UIMAException;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.CasIOUtils;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.LuaConsts;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.BorlandExport;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.ChangeMetaData;
import org.texttechnologylab.DockerUnifiedUIMAInterface.tools.RemoveMetaData;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.utilities.helper.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class Experiments {

    @Test
    public void ChatGPT_Verbs() throws Exception {

        String sourcePath = "/storage/projects/abrami/verbs/new";
        String outputPath = "/storage/projects/abrami/verbs/newoutput";
        String sourceSuffix = ".xmi";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, true);

        int iWorkers = Integer.valueOf(30);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("verbs" + iWorkers+ ".db")
                .withConnectionPoolSize(iWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, dockerDriver);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(ChangeMetaData.class))
                .withScale(iWorkers));

        composer.add(new DUUIDockerDriver.Component("docker.texttechnologylab.org/textimager-duui-spacy-single-de_core_news_sm:0.1.4")
                .withScale(iWorkers).withImageFetching());

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(RemoveMetaData.class))
                .withScale(iWorkers));

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, outputPath,
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, false,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(collectionReader, "chat_gpt_"+iWorkers);

    }

    @Test
    public void ChatGPT_Sum() throws Exception {

        String sourcePath = "/storage/projects/abrami/verbs/newoutput";
        String sourceSuffix = ".xmi.gz";

        AsyncCollectionReader collectionReader = new AsyncCollectionReader(sourcePath, sourceSuffix, 1, true);

        int iWorkers = Integer.valueOf(40);

        DUUILuaContext ctx = LuaConsts.getJSON();

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("verbs_export" + iWorkers+ ".db")
                .withConnectionPoolSize(iWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withStorageBackend(sqlite)
                .withLuaContext(ctx)
                .withSkipVerification(true)
                .withWorkers(iWorkers);

        DUUISwarmDriver swarm_driver = new DUUISwarmDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uima_driver = new DUUIUIMADriver()
                .withDebug(true);

        composer.addDriver(swarm_driver, uima_driver, dockerDriver);

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(BorlandExport.class)).build());

        composer.run(collectionReader, "chat_gpt_output_"+iWorkers);

    }

    @Test
    public void rename() throws IOException, UIMAException {

        String sInput = "/storage/projects/abrami/verbs/xmi/verbs";
        String sOutput = "/storage/projects/abrami/verbs/new";

        new File(sOutput).mkdir();

        Set<File> fSet = FileUtils.getFiles(sInput, ".xmi");
        Set<File> fSetOut = FileUtils.getFiles(sOutput, ".xmi");
        JCas pCas = JCasFactory.createJCas();
        JCas newCas = JCasFactory.createJCas();
        fSet.stream().filter(f->!fSetOut.contains(f)).sorted().forEach(f->{
            System.out.println(f.getName());
            pCas.reset();
            newCas.reset();
            try {
                CasIOUtils.load(new FileInputStream(f), pCas.getCas());

                DocumentMetaData dmd = DocumentMetaData.get(pCas);
                String sTitle = dmd.getDocumentTitle();
                String sID = dmd.getDocumentId();

                pCas.getDocumentText();

                AnnotationComment ac = new AnnotationComment(newCas);
                ac.setKey("created");
                ac.setValue("abrami");
                AnnotationComment content = new AnnotationComment(newCas);
                content.setKey("content");

                List<AnnotationComment> acList = JCasUtil.select(pCas, AnnotationComment.class).stream().filter(aC->{
                    return aC.getKey().equalsIgnoreCase("content");
                }).collect(Collectors.toList());

                String s = acList.get(0).getValue();
                content.setValue(s);
                ac.addToIndexes();
                content.addToIndexes();

                newCas.setDocumentText(sTitle+": "+pCas.getDocumentText());
                newCas.setDocumentLanguage("de");
                DocumentMetaData dmdNew = DocumentMetaData.create(newCas);
                dmdNew.setDocumentTitle(sTitle);
                dmdNew.setDocumentId(sID);

                CasIOUtils.save(newCas.getCas(), new FileOutputStream(new File(sOutput+"/"+sTitle.replace("/", "_")+".xmi")), SerialFormat.XMI);

            } catch (Exception e) {
                System.out.println(f.getName());
                e.printStackTrace();
            }

        });

    }

}

import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIKubernetesDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUISwarmDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUIAsynchronousProcessor;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUIFileReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestKubernetes {

    @Test
    public void testGenreClassla() throws Exception {
        // (NOT) DONE
        Path sourceLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy/6885333d-6d6d-4ae2-ae59-2fed02efb5b5/17901");
//        Path targetLocation = Paths.get("/storage/projects/CORE/azure/core-edutec-fileshare/texts_xmi_spacy_plus/genre_classla");
        Path targetLocation = Paths.get("/tmp/test");
        int scale = 80;

        DUUIAsynchronousProcessor processor = new DUUIAsynchronousProcessor(
                //new DUUIFileReader(
                new DUUIFileReader(
                        sourceLocation.toString(),
                        "html.gz.xmi.gz"
                        /*,
                        1
                        //,
//                        -1,
//                        false,
//                        "",
//                        false,
//                        null,
//                        -1,
//                        targetLocation.toString(),
//                        "html.gz.xmi.gz"

                         */
                )
        );

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withWorkers(scale)
                .withLuaContext(new DUUILuaContext().withJsonLibrary());

        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();
        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIKubernetesDriver kubernetesDriver = new DUUIKubernetesDriver();
        composer.addDriver(uimaDriver, swarmDriver, dockerDriver, kubernetesDriver);


        String model = "";
        model = "classla/xlm-roberta-base-multilingual-text-genre-classifier";
        //omposer.add(new DUUISwarmDriver.
        composer.add(new DUUIKubernetesDriver.
                        Component("docker.texttechnologylab.org/bfsrl_cuda:0.1.0")
//                .withParameter("model_name", model)
//                // .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
//                .withParameter("selection", "text")
                        .withScale(scale)
                        .withLabels("kubernetes.io/hostname=rohan")
                        //.withConstraintHost("isengart")
                        //.withLabels("hostname=isengart", "hostname=geltlin")
                        .build()
        );
//        composer.add(new DUUIKubernetesDriver.
//                Component("docker.texttechnologylab.org/duui-transformers-topic:latest")
//                .withParameter("model_name", model)
//                // .withParameter("selection", "de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence")
//                .withParameter("selection", "text")
//                .withScale(scale)
//                .withLabels("gpu=all")
//                //.withConstraintHost("isengart")
//                //.withLabels("hostname=isengart", "hostname=geltlin")
//                .build()
//        );
//composer.add(new DUUIKubernetesDriver.
//                Component("docker.texttechnologylab.org/duui-spacy-de_core_news_lg:latest")
//                .withScale(scale)
//                .withLabels("kubernetes.io/hostname=isengart")
//                .build()
//        );

        composer.add(new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class,
                XmiWriter.PARAM_TARGET_LOCATION, targetLocation.toString(),
                XmiWriter.PARAM_PRETTY_PRINT, true,
                XmiWriter.PARAM_OVERWRITE, true,
                XmiWriter.PARAM_VERSION, "1.1",
                XmiWriter.PARAM_COMPRESSION, "GZIP"
        )).build());

        composer.run(processor, "spacy_plus");
        composer.shutdown();
    }


}

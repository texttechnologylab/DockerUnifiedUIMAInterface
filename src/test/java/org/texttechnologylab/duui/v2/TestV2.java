package org.texttechnologylab.duui.v2;

import de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.junit.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIDockerDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIRemoteDriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIUIMADriver;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;

import java.net.URISyntaxException;

public class TestV2 {
    @Test
    public void test() throws Exception {


        DUUIComposer composer = new DUUIComposer()
                .withLuaContext(
                        new DUUILuaContext().withJsonLibrary()
                )
                .withSkipVerification(true)
                .addDriver(new DUUIRemoteDriver())
                .addDriver(new DUUIDockerDriver());

        composer.add(new DUUIRemoteDriver.Component("http://localhost:9714"));
//        composer.add(new DUUIDockerDriver.Component(
//                "duui-spacy-v2:dev"
//        ));

        JCas jCas = JCasFactory.createJCas();
        jCas.setDocumentText("Die Goethe Universität ist auf vier große Universitätsgelände über das Frankfurter Stadtgebiet verteilt.");
        jCas.setDocumentLanguage("de");
        Sentence sentence = new Sentence(jCas, 0, jCas.getDocumentText().length());
        sentence.addToIndexes();

        composer.run(jCas, "name");

        for (Annotation annotation : JCasUtil.select(jCas, Annotation.class)) {
            StringBuilder sb = new StringBuilder();
            annotation.prettyPrint(0, 2, sb, true);
            System.out.println(sb);
            System.out.println();
        }
    }
}

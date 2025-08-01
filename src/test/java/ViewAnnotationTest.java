import de.tudarmstadt.ukp.dkpro.core.api.ner.type.Person;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.SerialFormat;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.CasIOUtils;
import org.junit.Test;
import org.texttechnologylab.annotation.type.Fingerprint;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class ViewAnnotationTest {

    @Test
    public void test() throws ResourceInitializationException, CASException, IOException {

        JCas pCas = JCasFactory.createJCas();

        JCas pEins = pCas.createView("eins");
        JCas pZwei = pCas.createView("zwei");

        Person pPerson = new Person(pEins);
        pPerson.setBegin(0);
        pPerson.setEnd(0);
        pPerson.addToIndexes();

        Fingerprint fp = new Fingerprint(pZwei);
        fp.setCreate(System.currentTimeMillis());
        fp.setReference(pPerson);
        fp.setUser("me");
        fp.addToIndexes();

        CasIOUtils.save(pCas.getCas(), new FileOutputStream(new File("/tmp/test.xmi")), SerialFormat.XMI_1_1_PRETTY);

        pCas.reset();

        CasIOUtils.load(new FileInputStream(new File("/tmp/test.xmi")), pCas.getCas());

        JCasUtil.select(pCas.getView("zwei"), Fingerprint.class).forEach(f -> {
            System.out.println(f.getReference());
        });

    }

}

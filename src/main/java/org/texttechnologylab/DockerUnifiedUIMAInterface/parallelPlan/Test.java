package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.util.CasUtil;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.util.CasIOUtils;
import org.texttechnologylab.annotation.DocumentModification;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class Test {
    public static void main(String[] args) throws UIMAException, IOException, ClassNotFoundException {
        InputStream inputStream = new FileInputStream("test_corpora_xmi/3718079.xmi");
        InputStream inputStream2 = new FileInputStream("test_corpora_xmi/3718079.xmi");

        JCas jCas = JCasFactory.createJCas();
        System.out.println(jCas.getSofa());
        CasIOUtils.load(inputStream, jCas.getCas());
        System.out.println(jCas.getSofa());


        JCas jCas2 = JCasFactory.createJCas();
        CasIOUtils.load(inputStream2, jCas2.getCas());

        System.out.println(JCasUtil.select(jCas, Annotation.class).size());
        System.out.println(JCasUtil.select(jCas2, Annotation.class).size());

        //JCasUtil.selectAll(jCas).forEach(System.out::println);

        MergerFunctions.mergeAll(jCas, jCas2);
        System.out.println(JCasUtil.select(jCas2, Annotation.class).size());

//        JCas jCas3 = JCasFactory.createJCas();
//        MergerFunctions.mergeAll(jCas2, jCas3);
//        System.out.println(JCasUtil.select(jCas3, Annotation.class).size());


    }
}

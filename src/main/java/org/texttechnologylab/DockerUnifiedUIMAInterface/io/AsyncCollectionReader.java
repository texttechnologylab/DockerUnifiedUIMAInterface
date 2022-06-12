package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.resources.CompressionUtils;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncCollectionReader {
    private String _path;
    private ConcurrentLinkedQueue<String> _filePaths;
    private int _initialSize;
    private AtomicInteger _docNumber;

    public AsyncCollectionReader(String folder, String ending) {
        File fl = new File(folder);
        if(!fl.isDirectory()) {
            throw new RuntimeException("The folder is not a directory!");
        }

        _filePaths = new ConcurrentLinkedQueue<>();
        _path = folder;
        addFilesToConcurrentList(fl,ending,_filePaths);

        System.out.printf("Found %d files matching the pattern!\n",_filePaths.size());
        _initialSize = _filePaths.size();
        _docNumber = new AtomicInteger(0);
    }

    public boolean getNextCAS(JCas empty) throws IOException, CompressorException, SAXException {
        String result = _filePaths.poll();
        if(result==null) return false;
        int val = _docNumber.addAndGet(1);
        System.out.printf("Loading %d/%d\n",val,_initialSize);

        byte []file = Files.readAllBytes(Path.of(result));
        InputStream decodedFile;
        if(result.endsWith(".xz")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ,new ByteArrayInputStream(file));
        }
        else if(result.endsWith(".gz")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,new ByteArrayInputStream(file));
        }
        else {
            decodedFile = new ByteArrayInputStream(file);
        }
        XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true);
        return true;
    }

    public static void addFilesToConcurrentList(File folder, String ending, ConcurrentLinkedQueue<String> paths) {
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                if(listOfFiles[i].getName().endsWith(ending)) {
                    paths.add(listOfFiles[i].getPath().toString());
                }
            } else if (listOfFiles[i].isDirectory()) {
                addFilesToConcurrentList(listOfFiles[i],ending,paths);
            }
        }
    }
}

package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.resources.CompressionUtils;
import org.javaync.io.AsyncFiles;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class ByteReadFuture {
    private String _path;
    private byte[] _bytes;

    public ByteReadFuture(String path, byte[] bytes) {
        _path = path;
        _bytes = bytes;
    }

    public String getPath() {
        return _path;
    }

    public byte[] getBytes() {
        return _bytes;
    }
}

public class AsyncCollectionReader {
    private String _path;
    private ConcurrentLinkedQueue<String> _filePaths;
    private ConcurrentLinkedQueue<ByteReadFuture> _loadedFiles;
    private int _initialSize;
    private AtomicInteger _docNumber;
    private AtomicInteger _pendingLoadedFiles;

    private ProgressMeter progress = null;

    private int debugCount = 25;

    public AsyncCollectionReader(String folder, String ending) {
        this(folder, ending, 25, -1, false);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, boolean bSort) {
        this(folder, ending, debugCount, -1, bSort);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort) {
        File fl = new File(folder);
        if(!fl.isDirectory()) {
            throw new RuntimeException("The folder is not a directory!");
        }

        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _path = folder;
        addFilesToConcurrentList(fl,ending,_filePaths);

        if(bSort) {
            _filePaths = sortBySize(_filePaths);
        }

        if(iRandom>0){
            _filePaths = random(_filePaths, iRandom);
        }

        this.debugCount = debugCount;

        System.out.printf("Found %d files matching the pattern! \t Using Random: %d\n",_filePaths.size(), iRandom);
        _initialSize = _filePaths.size();
        _docNumber = new AtomicInteger(0);
        _pendingLoadedFiles = new AtomicInteger(0);

        progress = new ProgressMeter(_initialSize);
    }

    public int getCachedSize() {
        return _pendingLoadedFiles.get();
    }

    public boolean isEmpty() {
        return _docNumber.get() >= _initialSize;
    }

    public CompletableFuture<Integer> getAsyncNextByteArray() throws IOException, CompressorException, SAXException {
        String result = _filePaths.poll();
        if(result==null) return CompletableFuture.completedFuture(1);
        CompletableFuture<Integer> val = AsyncFiles
                .readAllBytes(Paths.get(result),1024*1024*5)
                .thenCompose(bytes -> {
                    _loadedFiles.add(new ByteReadFuture(result,bytes));
                    _pendingLoadedFiles.getAndAdd(1);
                    return CompletableFuture.completedFuture(0);
                });
        return val;
    }

    public boolean getNextCAS(JCas empty) throws IOException, CompressorException, SAXException {
        ByteReadFuture future = _loadedFiles.poll();

        byte []file = null;
        String result = null;
        if(future==null) {
            result = _filePaths.poll();
            if (result == null) return false;
        }
        else {
            _pendingLoadedFiles.decrementAndGet();
            result = future.getPath();
            file = future.getBytes();
        }
        int val = _docNumber.addAndGet(1);

        progress.setDone(val);
        progress.setLeft(_initialSize-val);

        if(_initialSize-progress.getCount()>debugCount) {
            if (val % debugCount == 0 || val == 0) {
                System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
           }
        }
        else{
            System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
        }

        if(file==null) {
            file = Files.readAllBytes(Path.of(result));
        }

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

    public static ConcurrentLinkedQueue<String> sortBySize(ConcurrentLinkedQueue<String> paths){

        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<String>();

        rQueue.addAll(paths.stream().sorted((s1, s2)->{
            Long firstLength = new File(s1).length();
            Long secondLength = new File(s2).length();

            return firstLength.compareTo(secondLength)*-1;
        }).collect(Collectors.toList()));

        return rQueue;

    }

    public static ConcurrentLinkedQueue<String> random(ConcurrentLinkedQueue<String> paths, int iRandom){

        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<String>();

        Random nRandom = new Random(iRandom);

        ArrayList<String> sList = new ArrayList<>();
        sList.addAll(paths);

        Collections.shuffle(sList, nRandom);

        if(iRandom>sList.size()){
            rQueue.addAll(sList.subList(0, sList.size()));
        }
        else{
            rQueue.addAll(sList.subList(0, iRandom));
        }



        return rQueue;

    }

    public static String getSize(String sPath){
        return FileUtils.byteCountToDisplaySize(new File(sPath).length());
    }
}

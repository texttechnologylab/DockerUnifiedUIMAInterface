package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.resources.CompressionUtils;
import org.javaync.io.AsyncFiles;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
        this(folder, ending, 25);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount) {
        File fl = new File(folder);
        if(!fl.isDirectory()) {
            throw new RuntimeException("The folder is not a directory!");
        }

        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _path = folder;
        addFilesToConcurrentList(fl,ending,_filePaths);

        System.out.printf("Found %d files matching the pattern!\n",_filePaths.size());
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
            if (val % debugCount == 0 || val == 1) {
                System.out.printf("%s: %s\n", progress, result);
            }
        }
        else{
            System.out.printf("%s: %s\n", progress, result);
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
}

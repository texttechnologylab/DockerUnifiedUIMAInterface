package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.utilities.helper.StringUtils;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    private ConcurrentLinkedQueue<String> _filePathsBackup;
    private ConcurrentLinkedQueue<ByteReadFuture> _loadedFiles;
    private int _initialSize;
    private AtomicInteger _docNumber;
    private long _maxMemory;
    private AtomicLong _currentMemorySize;

    private boolean _addMetadata = true;

    private String _language = null;

    private ProgressMeter progress = null;

    private int debugCount = 25;

    public AsyncCollectionReader(String folder, String ending) {
        this(folder, ending, 25, -1, false, "", false, null);
    }

    public AsyncCollectionReader(String folder, String ending, boolean bAddMetadata) {
        this(folder, ending, 25, -1, false, "", bAddMetadata, null);
    }

    public AsyncCollectionReader(String folder, String ending, boolean bAddMetadata, String language) {
        this(folder, ending, 25, -1, false, "", bAddMetadata, language);
    }

    public AsyncCollectionReader(String folder, String ending, String language) {
        this(folder, ending, 25, -1, false, "", false, language);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, boolean bSort) {
        this(folder, ending, debugCount, -1, bSort, "", false, null);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath){
        this(folder, ending, debugCount, iRandom, bSort, savePath, false, null);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata) {
        this(folder, ending, debugCount, iRandom, bSort, savePath, bAddMetadata, null);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language) {

        _addMetadata = bAddMetadata;
        _language = language;
        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _filePathsBackup = new ConcurrentLinkedQueue<>();

        if(new File(savePath).exists() && savePath.length()>0) {
            File sPath = new File(savePath);

            String sContent = null;
            try {
                sContent = StringUtils.getContent(sPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] sSplit = sContent.split("\n");

            for (String s : sSplit) {
                _filePaths.add(s);
            }

        }
        else{
            File fl = new File(folder);
            if (!fl.isDirectory()) {
                throw new RuntimeException("The folder is not a directory!");
            }


            _path = folder;
            addFilesToConcurrentList(fl, ending, _filePaths);
        }
        if(bSort) {
            _filePaths = sortBySize(_filePaths);
        }

        if(iRandom>0){
            _filePaths = random(_filePaths, iRandom);
        }

        if(savePath.length()>0){
            File nFile = new File(savePath);

            if(!nFile.exists()){
                StringBuilder sb = new StringBuilder();
                _filePaths.forEach(f->{
                    if(sb.length()>0){
                        sb.append("\n");
                    }
                    sb.append(f);
                });
                try {
                    StringUtils.writeContent(sb.toString(), nFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        _filePathsBackup.addAll(_filePaths);

        this.debugCount = debugCount;

        System.out.printf("Found %d files matching the pattern! \t Using Random: %d\n",_filePaths.size(), iRandom);
        _initialSize = _filePaths.size();
        _docNumber = new AtomicInteger(0);
        _currentMemorySize = new AtomicLong(0);
        // 500 MB
        _maxMemory = 500*1024*1024;

        progress = new ProgressMeter(_initialSize);

    }

    public void reset(){
        _filePaths = _filePathsBackup;
        _docNumber.set(0);
        progress = new ProgressMeter(_initialSize);
    }

    public AsyncCollectionReader withMaxMemorySize(long memorySize) {
        _maxMemory = memorySize;
        return this;
    }

    public long getMaxMemory() {
        return _maxMemory;
    }

    public long getCachedSize() {
        return _currentMemorySize.getAcquire();
    }

    public boolean isEmpty() {
        return _docNumber.get() >= _initialSize;
    }

    public CompletableFuture<Integer> getAsyncNextByteArray() throws IOException, CompressorException, SAXException {
        String result = _filePaths.poll();
        if(result==null) return CompletableFuture.completedFuture(1);
        CompletableFuture<Integer> val = AsyncFiles
                .readAllBytes(Paths.get(result),1024*1024*5)
                .thenApply(bytes -> {
                    _loadedFiles.add(new ByteReadFuture(result,bytes));

                    //Calculate estimated unpacked size by using a compression ratio of 0.1
                    long factor = 1;
                    if(result.endsWith(".gz")||result.endsWith(".xz")) {
                        factor = 10;
                    }
                    _currentMemorySize.getAndAdd(factor*(long)bytes.length);
                    return 0;
                });
        return val;
    }

    @SuppressWarnings("")
    public boolean getNextCAS(JCas empty) throws IOException, CompressorException, SAXException {
        ByteReadFuture future = _loadedFiles.poll();

        byte []file = null;
        String result = null;
        if(future==null) {
            result = _filePaths.poll();
            if (result == null) return false;
        }
        else {
            result = future.getPath();
            file = future.getBytes();
            long factor = 1;
            if(result.endsWith(".gz")||result.endsWith(".xz")) {
                factor = 10;
            }
            _currentMemorySize.getAndAdd(-factor*(long)file.length);
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

        try {
            XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true);
        }
        catch (Exception e){
            empty.setDocumentText(StringUtils.getContent(new File(result)));
        }

        if(_addMetadata) {
            if (JCasUtil.select(empty, DocumentMetaData.class).size() == 0) {
                DocumentMetaData dmd = DocumentMetaData.create(empty);
                File pFile = new File(result);
                dmd.setDocumentId(pFile.getName());
                dmd.setDocumentTitle(pFile.getName());
                dmd.setDocumentUri(pFile.getAbsolutePath());
                dmd.addToIndexes();
            }
        }

        if (_language != null && !_language.isEmpty()) {
            empty.setDocumentLanguage(_language);
        }

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

package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.io.ProgressMeter;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.annotation.SharedData;
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

    public enum DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE {
        RANDOM,
        SMALLEST,
        LARGEST
    }

    private static int getRandomFromMode(DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, int sampleSize) {
        if (sampleMode == DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.SMALLEST) {
            return sampleSize * -1;
        }
        return sampleSize;
    }

    private static boolean getSortFromMode(DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode) {
        if (mode == DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.RANDOM) {
            return false;
        }
        return true;
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int sampleSize, DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, String savePath, boolean bAddMetadata, String language) {
        this(folder, ending, debugCount, getRandomFromMode(sampleMode, sampleSize), getSortFromMode(sampleMode), savePath, bAddMetadata, language);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language) {
            this(folder, ending, debugCount, iRandom, bSort, savePath, bAddMetadata, language, 0);
    }

    /***
     * Constructor for the AsyncCollectionReader
     * @param folder Input folder
     * @param ending File ending
     * @param debugCount Number of documents to print out
     * @param iRandom Number of documents to select either randomly of from beginning or end depending on whether bSort is true or false
     * @param bSort Sort the documents by size from largest to smallest, if true and iRandom is not 0, the first (= largest) iRandom documents are selected, if iRandom is negative, the last (= smallest) iRandom documents are selected
     * @param savePath Path to a file where the paths of the selected documents are saved and loaded from, if the file exists
     * @param bAddMetadata Add metadata to the documents
     * @param language Add language to the documents
     * @param skipSmallerFiles Skip files smaller than this value in bytes
     */
    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles) {

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

            if (skipSmallerFiles > 0) {
                _filePaths = skipBySize(_filePaths, skipSmallerFiles);
            }
        }
        if(bSort) {
            _filePaths = sortBySize(_filePaths);
        }

        if (bSort && iRandom != 0) {
            System.out.println("Sorting and Random Selection is active, using the " + (iRandom > 0 ? "largest " : "smallest ") + Math.abs(iRandom) + " documents.");
            _filePaths = takeFirstOrLast(_filePaths, iRandom);
        }
        else if(iRandom>0){
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

    public static XmiSerializationSharedData deserialize(JCas pCas){

        XmiSerializationSharedData sharedData = null;
        SharedData result = JCasUtil.selectSingle(pCas, SharedData.class);

        if(result != null) {
            sharedData = XmiSerializationSharedData.deserialize(result.getValue());
        }
        return sharedData;

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
        } else if (result.endsWith(".gz")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(file));
        } else {
            decodedFile = new ByteArrayInputStream(file);
        }

        XmiCasDeserializer.deserialize(decodedFile, empty.getCas());

//        try {
//            XmiSerializationSharedData sharedData = deserialize(empty.getCas().getJCas());
//            XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true, sharedData);
//        }
//        catch (Exception e){
//            empty.setDocumentText(StringUtils.getContent(new File(result)));
//        }

        if (_addMetadata) {
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

    /**
     * Skips files smaller than skipSmallerFiles
     * @param paths paths to files
     * @param skipSmallerFiles skip files smaller than this value in bytes
     * @return filtered paths to files
     */
    public static ConcurrentLinkedQueue<String> skipBySize(ConcurrentLinkedQueue<String> paths, int skipSmallerFiles) {
        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<>();

        rQueue.addAll(paths
                        .stream()
                        .filter(s -> new File(s).length() >= skipSmallerFiles)
                        .collect(Collectors.toList())
        );

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

    /***
     * Takes the first n or last n elements of the queue
     * @param paths List of paths
     * @param n Number of elements to take, if n is positive, the first n elements are taken, if n is negative, the last n elements are taken, if n is 0, an IllegalArgumentException is thrown
     * @return A new queue with the first or last n elements
     */
    public static ConcurrentLinkedQueue<String> takeFirstOrLast(ConcurrentLinkedQueue<String> paths, int n){
        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<>();
        ArrayList<String> sList = new ArrayList<>(paths);

        if(n > 0){
            rQueue.addAll(sList.subList(0, n));
        }
        else if (n < 0){
            // NOTE using "+n" because the value is already negative
            rQueue.addAll(sList.subList(sList.size()+n, sList.size()));
        }
        else {
            throw new IllegalArgumentException("n must not be 0");
        }

        return rQueue;
    }

    public static String getSize(String sPath){
        return FileUtils.byteCountToDisplaySize(new File(sPath).length());
    }
}

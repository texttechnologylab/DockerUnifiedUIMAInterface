package org.texttechnologylab.DockerUnifiedUIMAInterface.io;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.dkpro.core.api.io.ProgressMeter;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.DUUIInputStream;
import org.texttechnologylab.DockerUnifiedUIMAInterface.document_handler.IDUUIDocumentHandler;
import org.texttechnologylab.utilities.helper.StringUtils;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Deprecated
public class AsyncCollectionReader {
    private String _path;
    private ConcurrentLinkedQueue<String> _filePaths;
    private ConcurrentLinkedQueue<String> _filePathsBackup;
    private ConcurrentLinkedQueue<DUUIInputStream> _loadedFilesStream;

    private ConcurrentLinkedQueue<ByteReadFuture> _loadedFiles;

    private int _initialSize;
    private AtomicInteger _docNumber;
    private long _maxMemory;
    private AtomicLong _currentMemorySize;

    private boolean _addMetadata = true;

    private String _targetPath = null;

    private String _language = null;

    private IDUUIDocumentHandler _dataReader;
    private ProgressMeter progress = null;

    private int debugCount = 25;

    /**
     * If a target location is specified, documents in the source directory that already exist in the target are skipped automatically
     */
    private String targetLocation = null;

    public static class Builder {

        private String _sourceDirectory;
        private String _sourceFileExtension;
        private IDUUIDocumentHandler _dataReader;
        private boolean _addMetadata = false;
        private int _debugCount = 25;
        private int _randomCount = -1;
        private boolean _sortBySize = false;
        private String _savePath = "";
        private String _language = null;
        private int _fileSizeBytes = 0;
        private String _targetDirectory = null;
        private String _targetFileExtension = "";

        public Builder withSourceDirectory(String sourceDirectory) {
            _sourceDirectory = sourceDirectory;
            return this;
        }

        public Builder withFileExtension(String fileExtension) {
            _sourceFileExtension = fileExtension;
            return this;
        }

        public Builder withDataReader(IDUUIDocumentHandler dataReader) {
            _dataReader = dataReader;
            return this;
        }

        public Builder withDebugCount(int debugCount) {
            _debugCount = debugCount;
            return this;
        }

        public Builder withRandomCount(int randomCount) {
            _randomCount = randomCount;
            return this;
        }

        public Builder withSortBySize(boolean sortBySize) {
            _sortBySize = sortBySize;
            return this;
        }

        public Builder withSavePath(String savePath) {
            _savePath = savePath;
            return this;
        }

        public Builder withAddMetadata(boolean addMetadata) {
            _addMetadata = addMetadata;
            return this;
        }

        public Builder withLanguage(String language) {
            _language = language;
            return this;
        }

        public Builder withSkipSmallerFiles(int fileSizeBytes) {
            _fileSizeBytes = fileSizeBytes;
            return this;
        }

        public Builder withTargetDirectory(String targetDirectory) {
            _targetDirectory = targetDirectory;
            return this;
        }

        public Builder withTargetFileExtension(String targetFileExtension) {
            _targetFileExtension = targetFileExtension;
            return this;
        }

        public AsyncCollectionReader build() {
            return new AsyncCollectionReader(
                _sourceDirectory,
                _sourceFileExtension,
                _dataReader,
                _debugCount,
                _randomCount,
                _sortBySize,
                _savePath,
                _addMetadata,
                _language,
                _fileSizeBytes,
                _targetDirectory,
                _targetFileExtension
            );
        }
    }

    public AsyncCollectionReader(String folder, String ending) {
        this(folder, ending, 25, -1, null, "", false, null, 0);
    }

    public AsyncCollectionReader(String folder, String ending, boolean bAddMetadata) {
        this(folder, ending, 25, -1, null, "", bAddMetadata, null, 0);
    }

    public AsyncCollectionReader(String folder, String ending, boolean bAddMetadata, String language) {
        this(folder, ending, 25, -1, null, "", bAddMetadata, language, 0);
    }



    public AsyncCollectionReader(String folder, String ending, String language) {
        this(folder, ending, 25, -1, null, "", false, language, 0);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, boolean bSort) {
        this(folder, ending, debugCount, -1, null, "", false, null, 0);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, boolean bSort, String sTargetPath) {
        this(folder, ending, debugCount, -1, bSort, "", false, null, sTargetPath, "xmi.gz");

    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath) {
        this(folder, ending, debugCount, iRandom, savePath, false, null, 0);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int sampleSize, DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles) {
        this(folder, ending, debugCount, getRandomFromMode(sampleMode, sampleSize), getSortFromMode(sampleMode), savePath, bAddMetadata, language, skipSmallerFiles, null, null);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int sampleSize, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles) {
        this(folder, ending, debugCount, sampleSize, null, savePath, bAddMetadata, language, skipSmallerFiles);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language) {
        this(folder, ending, debugCount, iRandom, bSort, savePath, bAddMetadata, language, null, null);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata) {
        this(folder, ending, debugCount, iRandom, bSort, savePath, bAddMetadata, null);
    }


    public AsyncCollectionReader(String folder, String ending, int debugCount, int sampleSize, DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, String savePath, boolean bAddMetadata, String language) {
        this(folder, ending, debugCount, getRandomFromMode(sampleMode, sampleSize), getSortFromMode(sampleMode), savePath, bAddMetadata, language);
    }

    /***
     * Constructor for the AsyncCollectionReader
     */

    public AsyncCollectionReader(String folder, String ending, IDUUIDocumentHandler dataReader, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles, String targetLocation, String targetEnding) {
        this.targetLocation = targetLocation;
        _addMetadata = bAddMetadata;
        _language = language;
        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _filePathsBackup = new ConcurrentLinkedQueue<>();
        _dataReader = dataReader;

        if (_dataReader != null) {
//            try {
//                if (!savePath.isEmpty()) {
//                    _filePaths.addAll(_dataReader.listDocuments(savePath));
//                }
//                _filePaths.addAll(_dataReader.listDocuments(folder));
//            } catch (IOException e) {
//                System.out.println("Save path not found. Processing all documents.");
//            }

        } else {
            if (new File(savePath).exists() && savePath.length() > 0) {
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

            } else {
                File fl = new File(folder);
                if (!fl.isDirectory()) {
                    throw new RuntimeException("The folder is not a directory!");
                }


                _path = folder;
                System.out.println("Search for files in :"+folder);
                addFilesToConcurrentList(fl, ending, _filePaths);

                if (skipSmallerFiles > 0) {
                    _filePaths = skipBySize(_filePaths, skipSmallerFiles);
                }
            }
        }

        if (skipSmallerFiles > 0) {
            _filePaths = skipBySize(_filePaths, skipSmallerFiles);
        }

//        if (new File(savePath).exists() && !savePath.isEmpty()) {
//            File sPath = new File(savePath);
//
//            String sContent = null;
//            try {
//                sContent = StringUtils.getContent(sPath);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            String[] sSplit = sContent.split("\n");
//            _filePaths.addAll(Arrays.asList(sSplit));
//
//        } else {
//            try {
//                _filePaths.addAll(_dataReader.listFiles(folder, ending));
//                if (skipSmallerFiles > 0) {
//                    _filePaths = skipBySize(_filePaths, skipSmallerFiles);
//                }
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }

//            if (_dataReader != null) {
//                try {
//                    _filePaths.addAll(_dataReader.listFiles(folder, ending));
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
//            } else {
//                File fl = new File(folder);
//                if (!fl.isDirectory()) {
//                    throw new RuntimeException();
//                }
//                _path = folder;
//                addFilesToConcurrentList(fl, ending, _filePaths);
//
//                if (skipSmallerFiles > 0) {
//                    _filePaths = skipBySize(_filePaths, skipSmallerFiles);
//                }
//            }

//        }

        if (bSort) {
            _filePaths = sortBySize(_filePaths);
        }

        if (bSort && iRandom >0) {
            System.out.println("Sorting and Random Selection is active, using the " + (iRandom > 0 ? "largest " : "smallest ") + Math.abs(iRandom) + " documents.");
            _filePaths = takeFirstOrLast(_filePaths, iRandom);
        } else if (iRandom > 0) {
            _filePaths = random(_filePaths, iRandom);
        }

        if (savePath.length() > 0) {
            File nFile = new File(savePath);

            if (!nFile.exists()) {
                StringBuilder sb = new StringBuilder();
                _filePaths.forEach(f -> {
                    if (sb.length() > 0) {
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

        // remove files that are already in the target location
        // NOTE we do this after saving the file list, as we do not want to change anything but only avoid processing files multiple times
        if (this.targetLocation != null) {
            _filePaths = removeIfInTarget(_filePaths, this.targetLocation, targetEnding, this._path, ending);
        }

        _filePathsBackup.addAll(_filePaths);

        this.debugCount = debugCount;

        System.out.printf("Found %d files matching the pattern! \t Using Random: %d\n", _filePaths.size(), iRandom);
        _initialSize = _filePaths.size();
        _docNumber = new AtomicInteger(0);
        _currentMemorySize = new AtomicLong(0);
        // 500 MB
        _maxMemory = 500 * 1024 * 1024;

        progress = new ProgressMeter(_initialSize);

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

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language, String targetLocation, String targetEnding) {
        this(folder, ending, null, debugCount, iRandom, bSort, savePath, bAddMetadata, language, 0, targetLocation, targetEnding);
    }

    public AsyncCollectionReader(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles, String targetLocation, String targetEnding) {
        this(folder, ending, null, debugCount, iRandom, bSort, savePath, bAddMetadata, language, skipSmallerFiles, targetLocation, targetEnding);
    }

    public enum DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE {
        RANDOM,
        SMALLEST,
        LARGEST
    }

    public void reset() {
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
        if (result == null) return CompletableFuture.completedFuture(1);
        CompletableFuture<Integer> val = AsyncFiles
                .readAllBytes(Paths.get(result), 1024 * 1024 * 5)
                .thenApply(bytes -> {
                    _loadedFiles.add(new ByteReadFuture(result, bytes));

                    //Calculate estimated unpacked size by using a compression ratio of 0.1
                    long factor = 1;
                    if (result.endsWith(".gz") || result.endsWith(".xz")) {
                        factor = 10;
                    }
                    _currentMemorySize.getAndAdd(factor * (long) bytes.length);
                    return 0;
                });
        return val;
    }
//    public CompletableFuture<Integer> getAsyncNextByteArray() throws IOException, CompressorException, SAXException {
//        String path = _filePaths.poll();
//        if (path == null) return CompletableFuture.completedFuture(1);
//
////        return CompletableFuture.supplyAsync(
////            () -> {
////                DUUIInputStream stream = _dataReader.readFile(path);
////                stream.getContent().readAllBytes();
////                return stream;
////            }
////        ).thenApply(stream -> {
////            _loadedFiles.add(stream);
////            long factor = 1;
////            if (path.endsWith(".gz") || path.endsWith(".xz")) {
////                factor = 10;
////            }
////            _currentMemorySize.getAndAdd(factor * (long) stream.getSizeBytes());
////            return 0;
////        });
//
//        if (_dataReader != null) {
//            String path = _filePaths.poll();
//            if (path == null) return CompletableFuture.completedFuture(1);
//
//            return CompletableFuture.supplyAsync(() -> _dataReader.readFile(path).getContent().readAllBytes()
//            ).thenApply(bytes -> {
//                _loadedFiles.add(new ByteReadFuture(path, bytes));
//                long factor = 1;
//                if (path.endsWith(".gz") || path.endsWith(".xz")) {
//                    factor = 10;
//                }
//                _currentMemorySize.getAndAdd(factor * (long) bytes.length);
//                return 0;
//            });
//        } else {
//            String result = _filePaths.poll();
//            if (result == null) return CompletableFuture.completedFuture(1);
//            return AsyncFiles
//                .readAllBytes(Paths.get(result), 1024 * 1024 * 5)
//                .thenApply(bytes -> {
//                    _loadedFiles.add(new ByteReadFuture(result, bytes));
//
//                    //Calculate estimated unpacked size by using a compression ratio of 0.1
//                    long factor = 1;
//                    if (result.endsWith(".gz") || result.endsWith(".xz")) {
//                        factor = 10;
//                    }
//                    _currentMemorySize.getAndAdd(factor * (long) bytes.length);
//                    return 0;
//                });
//        }
//    }

    @Deprecated
    public static XmiSerializationSharedData deserialize(JCas pCas) {

//        XmiSerializationSharedData sharedData = null;
//        SharedData result = JCasUtil.selectSingle(pCas, SharedData.class);
//
//        if (result != null) {
//            sharedData = XmiSerializationSharedData.deserialize(result.getValue());
//        }
//        return sharedData;

        return null;
    }

//    public boolean getNextCAS(JCas empty) throws IOException, CompressorException, SAXException {
//        DUUIInputStream stream = _loadedFiles.poll();
//
//        byte []file = null;
//        String result = null;
//        if (stream == null) {
//            result = _filePaths.poll();
//            if (result == null) return false;
//        } else {
//            result = stream.getName();
//            long factor = 1;
//            if (result.endsWith(".gz") || result.endsWith(".xz")) {
//                factor = 10;
//            }
//            _currentMemorySize.getAndAdd(-factor * stream.getSizeBytes());
//        }
//        int val = _docNumber.addAndGet(1);
//
//        progress.setDone(val);
//        progress.setLeft(_initialSize - val);
//
//        if (stream == null && _dataReader!=null) {
//            stream = _dataReader.readFile(result);
//        }
//        else{
//
//        }
//
//        String sizeBytes = FileUtils.byteCountToDisplaySize(stream.getSizeBytes());
//
//        if (_initialSize - progress.getCount() > debugCount) {
//            if (val % debugCount == 0 || val == 0) {
//                System.out.printf("%s: \t %s \t %s\n", progress, sizeBytes, result);
//            }
//        } else {
//            System.out.printf("%s: \t %s \t %s\n", progress, sizeBytes, result);
//        }
//
//        InputStream decodedFile;
//        if (result.endsWith(".xz")) {
//            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ, new ByteArrayInputStream(stream.getBytes()));
//        } else if (result.endsWith(".gz")) {
//            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(stream.getBytes()));
//        } else {
//            decodedFile = stream.getContent();
//        }
//
//        try {
//            XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true);
//        } catch (Exception e) {
//            System.out.println("WARNING: Could not deserialize file as XMI: " + result + " using plain text deserialization.");
//            empty.setDocumentText(new String(stream.getBytes(), StandardCharsets.UTF_8));
//        }
//
////        try {
////            XmiSerializationSharedData sharedData = deserialize(empty.getCas().getJCas());
////            XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true, sharedData);
////        }
////        catch (Exception e){
////            empty.setDocumentText(StringUtils.getContent(new File(result)));
////        }
//
//        if (_addMetadata) {
//            if (JCasUtil.select(empty, DocumentMetaData.class).isEmpty()) {
//                DocumentMetaData dmd = DocumentMetaData.create(empty);
//                dmd.setDocumentId(stream.getName());
//                dmd.setDocumentTitle(stream.getName());
//                dmd.setDocumentUri(stream.getPath());
//                dmd.addToIndexes();
//            }
//        }
//
//        if (_language != null && !_language.isEmpty()) {
//            empty.setDocumentLanguage(_language);
//        }
//
//        return true;
//    }

    public boolean getNextCAS(JCas empty) throws IOException, CompressorException, SAXException {
        ByteReadFuture future = _loadedFiles.poll();

        byte[] file = null;
        String result = null;
        if (future == null) {
            result = _filePaths.poll();
            if (result == null) return false;
        } else {
            result = future.getPath();
            file = future.getBytes();
            long factor = 1;
            if (result.endsWith(".gz") || result.endsWith(".xz")) {
                factor = 10;
            }
            _currentMemorySize.getAndAdd(-factor * (long) file.length);
        }
        int val = _docNumber.addAndGet(1);

        progress.setDone(val);
        progress.setLeft(_initialSize - val);

        if (_initialSize - progress.getCount() > debugCount) {
            if (val % debugCount == 0 || val == 0) {
                System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
            }
        } else {
            System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
        }

        if (file == null) {
            file = Files.readAllBytes(Path.of(result));
        }

        InputStream decodedFile;
        if (result.endsWith(".xz")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ, new ByteArrayInputStream(file));
        } else if (result.endsWith(".gz")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(file));
        } else {
            decodedFile = new ByteArrayInputStream(file);
        }

        XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true);

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

//        JCasUtil.select(empty, Sentence.class).size()
//        for (Sentence s : JCasUtil.select(empty, Sentence.class)) {
//            System.out.println(s.getCoveredText());
//        }

        return true;
    }
    public static void addFilesToConcurrentList(File folder, String ending, ConcurrentLinkedQueue<String> paths) {
        File[] listOfFiles = folder.listFiles();

        Arrays.stream(listOfFiles).parallel().forEach(f->{
            if(f.isFile()){
                if(f.getName().endsWith(ending)){
                    paths.add(f.getPath().toString());

                }
            }
            else if(f.isDirectory()){
                addFilesToConcurrentList(f, ending, paths);

            }
        });

//        for (int i = 0; i < listOfFiles.length; i++) {
//            if (listOfFiles[i].isFile()) {
//                if (listOfFiles[i].getName().endsWith(ending)) {
//                    paths.add(listOfFiles[i].getPath().toString());
//                }
//            } else if (listOfFiles[i].isDirectory()) {
//                addFilesToConcurrentList(listOfFiles[i], ending, paths);
//            }
//        }
    }

    public static ConcurrentLinkedQueue<String> sortBySize(ConcurrentLinkedQueue<String> paths) {

        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<String>();

        rQueue.addAll(paths.stream().sorted((s1, s2) -> {
            Long firstLength = new File(s1).length();
            Long secondLength = new File(s2).length();

            return firstLength.compareTo(secondLength) * -1;
        }).collect(Collectors.toList()));

        return rQueue;

    }

    /**
     * Skips files smaller than skipSmallerFiles
     *
     * @param paths            paths to files
     * @param skipSmallerFiles skip files smaller than this value in bytes
     * @return filtered paths to files
     */
    public static ConcurrentLinkedQueue<String> skipBySize(ConcurrentLinkedQueue<String> paths, int skipSmallerFiles) {
        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<>();

        System.out.println("Skip files smaller than " + skipSmallerFiles + " bytes");
        System.out.println("  Number of files before skipping: " + paths.size());

        rQueue.addAll(paths
            .stream()
            .filter(s -> new File(s).length() >= skipSmallerFiles)
            .collect(Collectors.toList())
        );

        System.out.println("  Number of files after skipping: " + rQueue.size());

        return rQueue;
    }

    public static ConcurrentLinkedQueue<String> random(ConcurrentLinkedQueue<String> paths, int iRandom) {

        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<String>();

        Random nRandom = new Random(iRandom);

        ArrayList<String> sList = new ArrayList<>();
        sList.addAll(paths);

        Collections.shuffle(sList, nRandom);

        if (iRandom > sList.size()) {
            rQueue.addAll(sList.subList(0, sList.size()));
        } else {
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
    public static ConcurrentLinkedQueue<String> takeFirstOrLast(ConcurrentLinkedQueue<String> paths, int n) {
        ConcurrentLinkedQueue<String> rQueue = new ConcurrentLinkedQueue<>();
        ArrayList<String> sList = new ArrayList<>(paths);

        System.out.println("Take first or last " + n + " files");
        System.out.println("  Number of files before taking: " + paths.size());

        if (n > 0) {
            rQueue.addAll(sList.subList(0, n));
        } else if (n < 0) {
            // NOTE using "+n" because the value is already negative
            rQueue.addAll(sList.subList(sList.size() + n, sList.size()));
        } else {
            throw new IllegalArgumentException("n must not be 0");
        }

        return rQueue;
    }

    /***
     * Removes files that are present in the target location
     * @param paths List of paths
     * @param targetLocation Target location where to check for files
     * @return A new queue without files that are present in the target location
     */
    public static ConcurrentLinkedQueue<String> removeIfInTarget(ConcurrentLinkedQueue<String> paths, String targetLocation, String targetEnding, String sourceLocation, String sourceEnding) {
        System.out.println("Chacking target location for files: " + targetLocation);
        ConcurrentLinkedQueue<String> targetFilePaths = new ConcurrentLinkedQueue<>();
        File targetDir = new File(targetLocation);
        if (!targetDir.exists()) {
            // This might not be an error, e.g. if it is the first run
            System.err.println("The targetLocation " + targetFilePaths + " does not exist! Continuing without removing files from target location.");
        } else if (targetDir.exists() && !targetDir.isDirectory()) {
            throw new RuntimeException("The targetLocation " + targetFilePaths + " is not a directory!");
        } else {
            addFilesToConcurrentList(targetDir, targetEnding, targetFilePaths);
        }
        System.out.println("Found " + targetFilePaths.size() + " files in target location");
	System.out.println("Source location has: " + paths.size());

        List<String> cleanList = new ArrayList<>();
        if (!targetFilePaths.isEmpty()) {
            System.out.println("Checking against " + targetFilePaths.size() + " files in target location");
            Set<String> existingFiles = targetFilePaths.stream()
                .map(Paths::get)
                .filter(Files::isRegularFile)
                .map(f -> targetDir.toPath().relativize(f).toString())
                .map(f -> f.replaceAll(targetEnding, ""))
                .map(f -> f.replaceAll(sourceEnding, ""))
                .collect(Collectors.toSet());

            Path sourceDir = Paths.get(sourceLocation);
            for (String f : paths) {
                Path p = Paths.get(f);
                String fn = sourceDir.relativize(p).toString();
                fn = fn.replaceAll(sourceEnding, "");
                boolean found = existingFiles.contains(fn);
                if (!found) {
                    cleanList.add(f);
                }
            }
        } else {
            System.out.println("No files in target location found, keeping all files from source location");
            cleanList.addAll(paths);
        }
        System.out.println("Removed " + (paths.size() - cleanList.size()) + " files from source location that are already present in target location, keeping " + cleanList.size() + " files");

        return new ConcurrentLinkedQueue<>(cleanList);
    }

    public static String getSize(String sPath) {
        return FileUtils.byteCountToDisplaySize(new File(sPath).length());
    }

    public static List<DUUIInputStream> getFilesInDirectoryRecursive(String directory) throws IOException {
        try (Stream<Path> stream = Files.walk(Paths.get(directory))) {
            return stream.filter(Files::isRegularFile).map(
                (path -> {
                    try (InputStream inputStream = new FileInputStream(path.toFile())) {
                        return new DUUIInputStream(
                            path.getFileName().toString(),
                            path.toString(),
                            path.toFile().length(),
                            new ByteArrayInputStream(inputStream.readAllBytes()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
            ).collect(Collectors.toList());
        }
    }
}

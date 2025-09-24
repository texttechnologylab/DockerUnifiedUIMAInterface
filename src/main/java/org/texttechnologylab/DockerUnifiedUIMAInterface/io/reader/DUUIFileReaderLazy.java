package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.javaync.io.AsyncFiles;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.ByteReadFuture;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.utilities.helper.ArchiveUtils;
import org.texttechnologylab.utilities.helper.StringUtils;
import org.texttechnologylab.utilities.helper.TempFileHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Lazy file reader. The reader reads in files, but processing can begin in parallel before the entire file list has been read in.
 *
 * @author Giuseppe Abrami
 */
public class DUUIFileReaderLazy implements DUUICollectionReader {

    protected String _path;
    protected ConcurrentLinkedQueue<String> _filePaths;
    protected ConcurrentLinkedQueue<String> _filePathsBackup;
    protected ConcurrentLinkedQueue<ByteReadFuture> _loadedFiles;

    protected AtomicInteger _docNumber;

    protected int _initialSize;
    protected AtomicInteger _skipNumber;
    protected AtomicInteger _repairedNumber;
    protected long _maxMemory;
    protected AtomicLong _currentMemorySize;

    protected AtomicInteger _collectionSize;

    protected boolean _addMetadata = true;

    protected String _targetPath = null;
    protected String _targetEnding;
    protected String _language = null;

    protected AdvancedProgressMeter progress = null;

    protected int debugCount = 25;

    protected String targetLocation = null;


    public DUUIFileReaderLazy(String folder, String ending) {
        this(folder, ending, 25, -1, null, "", false, null, 0);
    }

    public DUUIFileReaderLazy(String folder, String ending, int debugCount) {
        this(folder, ending, debugCount, -1, null, "", false, null, 0);
    }

    public DUUIFileReaderLazy(String folder, String ending, int debugCount, boolean bMetadata) {
        this(folder, ending, debugCount, -1, null, "", bMetadata, null, 0);
    }

    public DUUIFileReaderLazy(String folder, String ending, String sTargetPath) {
        this(folder, ending, 500, -1, false, "", true, null, 0, sTargetPath, ending);
    }

    public DUUIFileReaderLazy(String folder, String ending, String sTargetPath, String sTargetEnding) {
        this(folder, ending, 500, -1, false, "", true, null, 0, sTargetPath, sTargetEnding);
    }

    public DUUIFileReaderLazy(String folder, String ending, String sTargetPath, String sTargetEnding, int iDebugCount) {
        this(folder, ending, iDebugCount, -1, false, "", true, null, 0, sTargetPath, sTargetEnding);
    }

    public DUUIFileReaderLazy(String folder, String ending, String sTargetPath, int iDebugCount) {
        this(folder, ending, iDebugCount, -1, false, "", true, null, 0, sTargetPath, ending);
    }

    public DUUIFileReaderLazy(String folder, String ending, int debugCount, int sampleSize, AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles) {
        this(folder, ending, debugCount, getRandomFromMode(sampleMode, sampleSize), getSortFromMode(sampleMode), savePath, bAddMetadata, language, skipSmallerFiles, savePath, null);
    }

    public DUUIFileReaderLazy(String folder, String ending, int debugCount, int iRandom, boolean bSort, String savePath, boolean bAddMetadata, String language, int skipSmallerFiles, String targetLocation, String targetEnding) {
        this.targetLocation = targetLocation;
        this._targetEnding = targetEnding;
        _addMetadata = bAddMetadata;
        _language = language;
        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _filePathsBackup = new ConcurrentLinkedQueue<>();
        _collectionSize = new AtomicInteger(0);

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

            LazyFileReader lfr = new LazyFileReader(fl, ending, _filePaths, _collectionSize);

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


        this.debugCount = debugCount;

        System.out.printf("Starting lazy loading...");
        _docNumber = new AtomicInteger(0);
        _skipNumber = new AtomicInteger(0);
        _repairedNumber = new AtomicInteger(0);
        _currentMemorySize = new AtomicLong(0);
        // 500 MB
        _maxMemory = 500 * 1024 * 1024;

        progress = new AdvancedProgressMeter(_collectionSize.get());
    }

    private static int getRandomFromMode(AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE sampleMode, int sampleSize) {
        if (sampleMode == AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.SMALLEST) {
            return sampleSize * -1;
        }
        return sampleSize;
    }

    private static boolean getSortFromMode(AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE mode) {
        if (mode == AsyncCollectionReader.DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE.RANDOM) {
            return false;
        }
        return true;
    }

    public static void addFilesToConcurrentList(File folder, String ending, ConcurrentLinkedQueue<String> paths, AtomicInteger iCounter) {
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                if (listOfFiles[i].getName().endsWith(ending)) {
                    paths.add(listOfFiles[i].getPath().toString());
                    iCounter.incrementAndGet();
                }
            } else if (listOfFiles[i].isDirectory()) {
                addFilesToConcurrentList(listOfFiles[i], ending, paths, iCounter);
            }
        }
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


    public static String getSize(String sPath) {
        return FileUtils.byteCountToDisplaySize(new File(sPath).length());
    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return this.progress;
    }

    private static final Pattern XMLCharInvalidPattern =
            Pattern.compile(
                    "[\\x{1}-\\x{8}]|[\\x{B}-\\x{C}]|[\\x{E}-\\x{1F}]|[\\x{7F}-\\x{84}]|[\\x{86}-\\x{9F}]|[\\x{FDD0}-\\x{FDDF}]|[\\x{1FFFE}-\\x{1FFFF}]|[\\x{2FFFE}-\\x{2FFFF}]|[\\x{3FFFE}-\\x{3FFFF}]|[\\x{4FFFE}-\\x{4FFFF}]|[\\x{5FFFE}-\\x{5FFFF}]|[\\x{6FFFE}-\\x{6FFFF}]|[\\x{7FFFE}-\\x{7FFFF}]|[\\x{8FFFE}-\\x{8FFFF}]|[\\x{9FFFE}-\\x{9FFFF}]|[\\x{AFFFE}-\\x{AFFFF}]|[\\x{BFFFE}-\\x{BFFFF}]|[\\x{CFFFE}-\\x{CFFFF}]|[\\x{DFFFE}-\\x{DFFFF}]|[\\x{EFFFE}-\\x{EFFFF}]|[\\x{FFFFE}-\\x{FFFFF}]|[\\x{10FFFE}-\\x{10FFFF}]");

    @Override
    public void getNextCas(JCas empty) {

        boolean bSkip = false;
        String result = null;
        boolean bRepair = false;

        do {
            bSkip=false;
            bRepair = false;
            ByteReadFuture future = _loadedFiles.poll();

            byte[] file = null;
            if (future == null) {
                result = _filePaths.poll();
                if (result == null) return;
            } else {
                result = future.getPath();
                file = future.getBytes();
                long factor = 1;
                if (result.endsWith(".gz") || result.endsWith(".xz")) {
                    factor = 10;
                }
                _currentMemorySize.getAndAdd(-factor * (long) file.length);
            }


            if (file == null) {
                try {
                    file = Files.readAllBytes(Path.of(result));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            InputStream decodedFile = null;
            try {


                if (result.endsWith(".xz")) {
                    decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ, new ByteArrayInputStream(file));
                } else if (result.endsWith(".gz")) {
                    decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(file));
                } else if (result.endsWith(".bz2")) {
                    decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, new ByteArrayInputStream(file));
                } else if (result.endsWith(".xmi")) {
                    decodedFile = new ByteArrayInputStream(file);
                } else {
                    empty.setDocumentText(IOUtils.toString(new FileInputStream(new File(result)), StandardCharsets.UTF_8));
                }
                // check

                if (empty != null && empty.getDocumentText() == null) {
                    XmiCasDeserializer.deserialize(decodedFile, empty.getCas(), true);
                }
                try {
                    int iLength = empty.getDocumentText().length();
                } catch (Exception e) {
                    throw new SAXParseException(e.getMessage(), null);
                }
            } catch (Exception e) {

                if(e instanceof SAXParseException){
                    try {
                        String rString = "";
                        if(result.endsWith(".gz")){
                            File decompressedFile = ArchiveUtils.decompressGZ(new File(result));
                            rString = IOUtils.toString(new FileInputStream(decompressedFile), StandardCharsets.UTF_8);
                            decompressedFile.delete();
                        } else if (result.endsWith(".xmi")) {
                            rString = IOUtils.toString(decodedFile, StandardCharsets.UTF_8);
                        } else {
                            rString = org.texttechnologylab.utilities.helper.FileUtils.getContentFromFile(new File(result));
                        }
//                        System.out.println(rString);

                        System.out.println("Reparing " + result);
                        rString = XMLCharInvalidPattern.matcher(rString).replaceAll("");

                        if (!result.endsWith(".txt")) {
                            File tFile = TempFileHandler.getTempFile("aaa", ".xmi");
                            org.texttechnologylab.utilities.helper.FileUtils.writeContent(rString, tFile);

                            XmiCasDeserializer.deserialize(new FileInputStream(tFile), empty.getCas(), true);
                            tFile.delete();

                        } else {
                            empty.setDocumentText(rString);
                        }

//                        System.out.println("Repaired File: "+result+"\t"+empty.getDocumentText().length());

                        _repairedNumber.incrementAndGet();
                        bRepair = true;

                    } catch (IOException ex) {
                        ex.printStackTrace();
                    } catch (SAXException ex) {
                        ex.printStackTrace();
                    }
                }
                else{
                    e.printStackTrace();
                }

            }

            if(this.targetLocation.length()>0) {
                try {
                    DocumentMetaData dmd = DocumentMetaData.get(empty);
                    if (dmd != null) {
                        String sURI = dmd.getDocumentUri();
                        String sBase = "";
                        if (dmd.getDocumentBaseUri() != null) {
                            sBase = dmd.getDocumentBaseUri();
                        }
                        File tFile = null;
                        String sNewOutput = "";
                        if (sBase.length() > 0) {
                            sNewOutput = sURI.replace(sBase, this.targetLocation) + this._targetEnding;
                            tFile = new File(sNewOutput);
                        } else {
                            tFile = new File(this.targetLocation + "/" + dmd.getDocumentId() + "." + this._targetEnding);
                            sNewOutput = dmd.getDocumentId() + "." + this._targetEnding;
                        }
                        System.out.println(tFile.getAbsolutePath());
                        if(tFile.exists()){
                            bSkip = true;
                            _skipNumber.incrementAndGet();
                            if (_skipNumber.get() % 100 == 0) {
                                System.out.println("Skip: (" + _skipNumber.get() + ")\t" + sNewOutput);
                            }
                            // clear document for next decoding
                            empty.reset();
                        }
                    }
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }

            int val = _docNumber.addAndGet(1);

            int iCurSize = _collectionSize.get();

            progress.setMax(iCurSize);

            progress.setDone(val);
            progress.setLeft(iCurSize - val);

//            if(bSkip && _skipNumber.get()%debugCount==0){
//                System.out.printf("skip\t (%d) \t %s \t %s\n", _docNumber.get(), progress, result);
//            }
//            else {
                if (iCurSize - progress.getCount() > debugCount) {
                    if (val % debugCount == 0 || val == 0) {
                        System.out.printf("%s: \t %s \t %s\t (S: %s / R: %s)\n", progress, getSize(result), result, _skipNumber.get(), _repairedNumber.get());
                    }
                } else {
                    System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
                }
//            }

        }
        while(bSkip);

        if (_addMetadata) {
            if (JCasUtil.select(empty, DocumentMetaData.class).size() == 0) {
                DocumentMetaData dmd = new DocumentMetaData(empty);
                File pFile = new File(result);
                dmd.setDocumentId(pFile.getName());
                dmd.setDocumentTitle(pFile.getName());
                dmd.setDocumentUri(pFile.getAbsolutePath());
                dmd.setDocumentBaseUri(this._path);
                dmd.addToIndexes();
            }
        }

        if (_language != null && !_language.isEmpty()) {
            empty.setDocumentLanguage(_language);
        }

    }

    public void reset() {
        _filePaths = _filePathsBackup;
        _docNumber.set(0);
        progress = new AdvancedProgressMeter(_collectionSize.get());
    }

    @Override
    public boolean hasNext() {
        return _filePaths.size() > 0;
    }

    @Override
    public long getSize() {
        return _filePaths.size();
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

    @Override
    public long getDone() {
        return _docNumber.get();
    }

    public String formatSize(long lSize) {

        int u = 0;
        for (; lSize > 1024 * 1024; lSize >>= 10) {
            u++;
        }
        if (lSize > 1024)
            u++;
        return String.format("%.1f %cB", lSize / 1024f, " kMGTPE".charAt(u));

    }

    public enum DUUI_ASYNC_COLLECTION_READER_SAMPLE_MODE {
        RANDOM,
        SMALLEST,
        LARGEST
    }

    class LazyFileReader {

        boolean bRunning = true;

        File pFile = null;
        String sEnding = "";

        ConcurrentLinkedQueue<String> _filePaths = null;

        AtomicInteger iCounter = null;

        public LazyFileReader(File fl, String ending, ConcurrentLinkedQueue<String> _filePaths, AtomicInteger iCounter) {
            this.pFile = fl;
            this.sEnding = ending;
            this._filePaths = _filePaths;
            this.iCounter = iCounter;

            Runnable r = new Runnable() {
                @Override
                public void run() {
                    addFilesToConcurrentList(pFile, sEnding, _filePaths, iCounter);
                }
            };
            Thread pThread = new Thread(r);
            pThread.start();


        }

//        public LazyFileReader(File fl, String ending, String sTargetPath, String sTargetEnding, ConcurrentLinkedQueue<String> _filePaths, AtomicInteger iCounter) {
//
//            this(fl, ending, _filePaths, iCounter);
//
////            try {
////                Thread.sleep(1000l);
////            } catch (InterruptedException e) {
////                throw new RuntimeException(e);
////            }
//
//            if(sTargetPath.length()>0) {
//
//                try {
//                    JCas checkCas = JCasFactory.createJCas();
//                    Set<String> checkString = new HashSet<>(0);
//                    Runnable r = new Runnable() {
//                        @Override
//                        public void run() {
//                            String result = null;
//                            byte[] file = null;
//
//                            while (hasNext()) {
//                                checkCas.reset();
//                                String sPath = _filePaths.poll();
//
//                                if(checkString.contains(sPath)){
//                                    continue;
//                                }
//
//                                checkString.add(sPath);
//
//                                try {
//                                    file = Files.readAllBytes(Path.of(sPath));
//                                } catch (IOException e) {
//                                    throw new RuntimeException(e);
//                                }
//
//                                InputStream decodedFile = null;
//                                try {
//                                    if (sPath.endsWith(".xz")) {
//                                        decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ, new ByteArrayInputStream(file));
//                                    } else if (sPath.endsWith(".gz")) {
//                                        decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, new ByteArrayInputStream(file));
//                                    } else if (sPath.endsWith(".bz2")) {
//                                        decodedFile = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, new ByteArrayInputStream(file));
//                                    } else {
//                                        decodedFile = new ByteArrayInputStream(file);
//                                    }
//
//                                    XmiCasDeserializer.deserialize(decodedFile, checkCas.getCas(), true);
//                                } catch (CompressorException ex) {
//                                    throw new RuntimeException(ex);
//                                } catch (IOException ex) {
//                                    throw new RuntimeException(ex);
//                                } catch (SAXException ex) {
//                                    throw new RuntimeException(ex);
//                                }
//
//
//                                if (sTargetPath.length() > 0) {
//                                    try {
//                                        DocumentMetaData dmd = DocumentMetaData.get(checkCas);
//                                        if (dmd != null) {
//                                            String sURI = dmd.getDocumentUri();
//                                            String sBase = dmd.getDocumentBaseUri();
//                                            String sNewOutput = sURI.replace(sBase, sTargetPath) + sTargetEnding;
//                                            File tFile = new File(sNewOutput);
//                                            if (!tFile.exists()) {
//                                                _filePaths.add(sPath);
//                                            }
//                                        }
//                                    } catch (Exception e) {
//                                        e.printStackTrace();
//                                    }
//                                }
//
//                            }
//                        }
//
//                    };
//
//                    Thread pThread = new Thread(r);
//                    pThread.start();
//                } catch (ResourceInitializationException ex) {
//                    throw new RuntimeException(ex);
//                } catch (CASException ex) {
//                    throw new RuntimeException(ex);
//                }
//
//            }
//
//
//        }

    }


}

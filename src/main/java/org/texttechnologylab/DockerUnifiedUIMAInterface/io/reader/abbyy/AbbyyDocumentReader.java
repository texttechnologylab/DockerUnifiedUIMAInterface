package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy;

import com.google.common.base.Strings;
import de.tudarmstadt.ukp.dkpro.core.api.anomaly.type.Anomaly;
import de.tudarmstadt.ukp.dkpro.core.api.anomaly.type.SuggestedAction;
import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.uima.UIMARuntimeException;
import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.descriptor.ExternalResource;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSArray;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.dkpro.core.api.parameter.ComponentParameters;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.AntPathMatcher;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.FileType;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.MatchRules;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Patterns;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.Utils;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.bb.RelativeBoundingBox;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.FineReaderEventHandler;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.ProgressTracker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.ProgressTrackerRemaining;
import org.texttechnologylab.annotation.ocr.abbyy.Document;
import org.texttechnologylab.annotation.ocr.abbyy.Line;
import org.texttechnologylab.annotation.ocr.abbyy.Page;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Reader class for ABBYY FineReader XMLs. Handles raw or compressed input files (GZip, BZip2, and XZ).
 * Use {@link #PARAM_ROOT_PATTERNS PARAM_ROOT_PATTERNS} to define the root folders of documents within the
 * {@link #PARAM_SOURCE_LOCATION PARAM_SOURCE_LOCATION}, and {@link #PARAM_FILE_PATTERNS PARAM_FILE_PATTERNS}
 * to filter files within these directories.
 * <p/>
 * The CAS'es produced by this reader will have their {@link DocumentMetaData DocumentMetaData} set according to the
 * root path. You may set the base URI using the {@link #PARAM_BASE_URI PARAM_BASE_URI}.
 * It will replace the path prefix <emph>above</emph> the root path.
 * The documents will have their {@link DocumentMetaData#setDocumentId DocumentId} set to the name of their respective
 * root directory. You can als pass a {@link #PARAM_COLLECTION_ID collection ID} to be set.
 * <p/>
 * Each element parsed from the ABBYY FineReader files will have some additional metadata set in accordance with the
 * BIOfid project's export structure (which in turn, reflects the Visual Library structure),
 * i.e. the URI of a {@link Page#getUri() Page} elements will point to {@code $BASE_URI/$PAGE_NAME} where the
 * {@code PAGE_NAME} is parsed from the file name (e.g. {@code 01_1234567.xml}).
 */
public class AbbyyDocumentReader extends JCasCollectionReader_ImplBase {

    /**
     * Location from which the input is read.
     */
    public static final String PARAM_SOURCE_LOCATION = ComponentParameters.PARAM_SOURCE_LOCATION;
    @ConfigurationParameter(name = PARAM_SOURCE_LOCATION, mandatory = true)
    private String sourceLocation;

    /**
     * A set of Ant-like include/exclude patterns. A pattern starts with {@link MatchRules#INCLUDE_PREFIX [+]}
     * if it is an include pattern and with {@link MatchRules#EXCLUDE_PREFIX [-]} if it is an exclude pattern.
     * The wildcard <code>&#47;**&#47;</code> can be used to address any number of sub-directories.
     * The wildcard {@code *} can be used to a address a part of a name.
     * <p/>
     * This pattern is used to filter the <emph>directories</emph> considered document roots.
     * If omitted, will include all directories in the {@link #PARAM_SOURCE_LOCATION sourceLocation}
     * (equvialent to {@link #DEFAULT_ROOT_INCLUDE [+]*}).
     */
    public static final String PARAM_ROOT_PATTERNS = "rootPatterns";
    @ConfigurationParameter(name = PARAM_ROOT_PATTERNS, mandatory = false)
    private String[] rootPatterns;
    public static final String DEFAULT_ROOT_INCLUDE = "*";

    /**
     * A set of Ant-like include/exclude patterns. A pattern starts with {@link MatchRules#INCLUDE_PREFIX [+]}
     * if it is an include pattern and with {@link MatchRules#EXCLUDE_PREFIX [-]} if it is an exclude pattern.
     * The wildcard <code>&#47;**&#47;</code> can be used to address any number of sub-directories.
     * The wildcard {@code *} can be used to a address a part of a name.
     * <p/>
     * This pattern is used to filter the <emph>files</emph> considered as input.
     * If omitted, will include all XML files in any sub-directory of the {@link #PARAM_SOURCE_LOCATION sourceLocation}
     * (equivalent to {@link #DEFAULT_FILE_INCLUDE [+]**<code>/</code>*.xml*}).
     */
    public static final String PARAM_FILE_PATTERNS = ComponentParameters.PARAM_PATTERNS;
    @ConfigurationParameter(name = PARAM_FILE_PATTERNS, mandatory = false)
    private String[] filePatterns;
    public static final String DEFAULT_FILE_INCLUDE = "**/*.xml*";

    /**
     * Overwrite the base URI of the parsed documents.
     * If omitted, will default to the parent directory of the document root path.
     */
    public static final String PARAM_BASE_URI = "baseUri";
    @ConfigurationParameter(name = PARAM_BASE_URI, mandatory = false)
    private String baseUri;

    /**
     * If provided, {@link DocumentMetaData#setCollectionId(String) sets the DocumentMetaData.CollectionId}.
     */
    public static final String PARAM_COLLECTION_ID = "collectionId";
    @ConfigurationParameter(name = PARAM_COLLECTION_ID, mandatory = false)
    private String collectionId;

    /**
     * Name of optional external (UIMA) resource that contains the Locator for a (Spring)
     * ResourcePatternResolver implementation for locating (spring) resources.
     * <p/>
     * Defaults to {@link PathMatchingResourcePatternResolver}.
     */
    public static final String KEY_RESOURCE_RESOLVER = "resolver";
    @ExternalResource(key = KEY_RESOURCE_RESOLVER, mandatory = false)
    private final ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    /**
     * Set this as the language of the produced documents. Default: {@code de}.
     */
    public static final String PARAM_LANGUAGE = ComponentParameters.PARAM_LANGUAGE;
    @ConfigurationParameter(name = PARAM_LANGUAGE, mandatory = false, defaultValue = "de")
    private String language;

    /**
     * A RegEx {@link Pattern pattern string} to use on the document root sub-path to determine the document ID.
     * If the RegEx contains multiple (alternative) match groups, the first matching group will be used.
     * Defaults to {@code .*\/(\d+)|(\d+)[^/]*}.
     */
    public static final String PARAM_DOCUMENT_ID_PATTERN = "documentIdPatternString";
    @ConfigurationParameter(name = PARAM_DOCUMENT_ID_PATTERN, mandatory = false, defaultValue = ".*/(\\d+)|(\\d+)[^/]*")
    protected String documentIdPatternString;
    protected Pattern documentIdPattern;

    /**
     * Optional {@link Pattern Pattern} flags to use for the document ID pattern.
     */
    public static final String PARAM_DOCUMENT_ID_PATTERN_FLAGS = "documentIdPatternFlags";
    @ConfigurationParameter(name = PARAM_DOCUMENT_ID_PATTERN_FLAGS, mandatory = false)
    protected int documentIdPatternFlags;

    /**
     * A RegEx {@link Pattern pattern string} to use on the page file name to determine the page index and ID.
     * <p/>
     * Use <b>named capturing groups</b> {@code (?<index>)} and {@code (?<id>)} for page index and ID, respectively.
     * The index will be parsed as an integer while the ID remains unchanged.
     * Defaults to {@code ^\(?<index>d+)_(?<id>\d+).*}.
     */
    public static final String PARAM_PAGE_ID_PATTERN = "pageIdPatternString";
    @ConfigurationParameter(name = PARAM_PAGE_ID_PATTERN, mandatory = false, defaultValue = "^(?<index>\\d+)_(?<id>\\d+).*")
    protected String pageIdPatternString;
    protected Pattern pageIdPattern;

    /**
     * Optional {@link Pattern Pattern} flags to use for the page ID pattern.
     */
    public static final String PARAM_PAGE_ID_PATTERN_FLAGS = "pageIdPatternFlags";
    @ConfigurationParameter(name = PARAM_PAGE_ID_PATTERN_FLAGS, mandatory = false)
    protected int pageIdPatternFlags;

    /**
     * Set to {@code false} to disable adding {@link org.texttechnologylab.annotation.ocr.abbyy.Page Page}
     * annotations to the output CAS.
     */
    public static final String PARAM_ADD_PAGES = "addPages";
    @ConfigurationParameter(name = PARAM_ADD_PAGES, mandatory = false, defaultValue = "true")
    protected Boolean addPages;

    /**
     * Set to {@code false} to disable adding {@link org.texttechnologylab.annotation.ocr.abbyy.Block Block}
     * annotations to the output CAS.
     */
    public static final String PARAM_ADD_BLOCKS = "addBlocks";
    @ConfigurationParameter(name = PARAM_ADD_BLOCKS, mandatory = false, defaultValue = "true")
    protected Boolean addBlocks;

    /**
     * Set to {@code false} to disable adding {@link org.texttechnologylab.annotation.ocr.abbyy.Paragraph Paragraph}
     * annotations to the output CAS.
     */
    public static final String PARAM_ADD_PARAGRAPHS = "addParagraphs";
    @ConfigurationParameter(name = PARAM_ADD_PARAGRAPHS, mandatory = false, defaultValue = "true")
    protected Boolean addParagraphs;

    /**
     * Set to {@code false} to disable adding {@link org.texttechnologylab.annotation.ocr.abbyy.Line Line}
     * annotations to the output CAS.
     */
    public static final String PARAM_ADD_LINES = "addLines";
    @ConfigurationParameter(name = PARAM_ADD_LINES, mandatory = false, defaultValue = "true")
    protected Boolean addLines;

    /**
     * Set to {@code false} to disable adding {@link org.texttechnologylab.annotation.ocr.abbyy.Line Line} {@link org.texttechnologylab.annotation.ocr.abbyy.Format Format}
     * annotations to the output CAS.
     */
    public static final String PARAM_ADD_LINE_FORMAT = "addLineFormat";
    @ConfigurationParameter(name = PARAM_ADD_LINE_FORMAT, mandatory = false, defaultValue = "true")
    protected Boolean addLineFormat;

    /**
     * Set to {@code false} to disable adding {@link org.texttechnologylab.annotation.ocr.abbyy.Token Token}
     * annotations to the output CAS.
     */
    public static final String PARAM_ADD_TOKENS = "addTokens";
    @ConfigurationParameter(name = PARAM_ADD_TOKENS, mandatory = false, defaultValue = "true")
    protected Boolean addTokens;

    /**
     * If set to {@code true}, enable heuristics to detect "garbage" lines in OCR output.
     */
    public static final String PARAM_ENABLE_GARBAGE_HEURISTICS = "enableGarbageHeuristics";
    @ConfigurationParameter(name = PARAM_ENABLE_GARBAGE_HEURISTICS, mandatory = false, defaultValue = "false")
    protected Boolean enableGarbageHeuristics;

    /**
     * Unless set to {@code false}, HTML escaped characters in the OCR output will be unescaped.
     */
    public static final String PARAM_UNESCAPE_HTML = "unescapeHTML";
    @ConfigurationParameter(name = PARAM_UNESCAPE_HTML, mandatory = false, defaultValue = "true")
    protected Boolean unescapeHTML;

    /**
     * Unless set to {@code false}, all consecutive whitespace characters will be replaced by a single regular whitespace.
     */
    public static final String PARAM_UNIFY_WHITESPACES = "unifyWhitespaces";
    @ConfigurationParameter(name = PARAM_UNIFY_WHITESPACES, mandatory = false, defaultValue = "true")
    protected Boolean unifyWhitespaces;

    /**
     * If given, will only add elements that are <strong>fully confined</strong> by these <em>relative</em> bounding box
     * coordinates (in domain [0, 100]). Can be an array of one to four values which are interpreted the same way as
     * HTML padding/margin is specified:
     * <ol>
     * <li>{all four sides}</li>
     * <li>{top/bottom, right/left}</li>
     * <li>{top, right/left, bottom}</li>
     * <li>{top, right, bottom, left}</li>
     * </ol>
     * <p/>
     * Example: {@code RelativeBoundingBox.fromString("5,1") -> (5, 99, 95, 1)}
     */
    public static final String PARAM_BOUNDING_BOX_DEF = "boundingBoxDef";
    @ConfigurationParameter(name = PARAM_BOUNDING_BOX_DEF, mandatory = false)
    protected String boundingBoxDef;
    protected RelativeBoundingBox boundingBox;

    /**
     * The frequency with which read documents are logged.
     * <p>
     * Set to 0 or negative values to deactivate logging.
     */
    public static final String PARAM_LOG_FREQ = "logFreq";
    @ConfigurationParameter(name = PARAM_LOG_FREQ, mandatory = true, defaultValue = "1000")
    private int logFreq;

    /**
     * Unless set, errors during XML parsing will be logged but processing will continue with the next document.
     */
    public static final String PARAM_STOP_AT_FIRST_ERROR = "stopAtFirstError";
    @ConfigurationParameter(name = PARAM_STOP_AT_FIRST_ERROR, mandatory = true, defaultValue = "false")
    private boolean stopAtFirstError;

    private ProgressTracker progressDocuments;
    private ProgressTrackerRemaining progressFiles;

    private TreeMap<Path, List<Path>> resourceMap;
    private Iterator<Map.Entry<Path, List<Path>>> nestedResourceIterator;

    private final AntPathMatcher matcher = new AntPathMatcher();
    private SAXParser saxParser;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        if (!Path.of(sourceLocation).toFile().isDirectory()) {
            throw new ResourceInitializationException(
                    new IOException(
                            String.format(
                                    "PARAM_SOURCE_LOCATION does not point to a valid directory: %s",
                                    sourceLocation
                            )
                    )
            );
        }

        if (Strings.isNullOrEmpty(this.baseUri)) {
            this.baseUri = sourceLocation;
        }
        if (!this.baseUri.endsWith("/")) {
            this.baseUri += "/";
        }

        this.documentIdPattern = Pattern.compile(this.documentIdPatternString, this.documentIdPatternFlags);
        this.pageIdPattern = Pattern.compile(this.pageIdPatternString, this.pageIdPatternFlags);

        if (boundingBoxDef != null) {
            boundingBox = RelativeBoundingBox.fromString(boundingBoxDef);
        }

        try {
            saxParser = SAXParserFactory.newInstance().newSAXParser();
        } catch (ParserConfigurationException | SAXException e) {
            throw new ResourceInitializationException(e);
        }

        MatchRules rootRules = new MatchRules(rootPatterns, DEFAULT_ROOT_INCLUDE);
        MatchRules fileRules = new MatchRules(filePatterns, DEFAULT_FILE_INCLUDE);

        List<Path> rootPaths = scan(sourceLocation, rootRules, FileType.OnlyDirectories);

        resourceMap = new TreeMap<>();
        for (Path rootPath : rootPaths) {
            List<Path> filePaths = scan(rootPath.toString(), fileRules, FileType.OnlyFiles);
            if (!filePaths.isEmpty()) {
                resourceMap.put(rootPath, filePaths);
            }
        }
        nestedResourceIterator = resourceMap.entrySet().iterator();

        // Check if we found any root directories and corresponding files
        if (rootPaths.isEmpty()) {
            throw new ResourceInitializationException(new NoSuchElementException(
                    "Could not find any root directories in the source location: " + sourceLocation
            ));
        }
        int totalNumberOfFiles = resourceMap.values().stream().map(List::size).reduce(0, Integer::sum);
        if (totalNumberOfFiles == 0) {
            throw new ResourceInitializationException(
                    new NoSuchElementException(
                            String.format("None of the root %d directories contained any matching files!", rootPaths.size())
                    )
            );
        }

        progressDocuments = new ProgressTracker(rootPaths.size(), "documents");
        progressFiles = new ProgressTrackerRemaining(totalNumberOfFiles, "files");
    }

    private List<Path> scan(String basePath, MatchRules matchRules, FileType fileType) {
        final String basePathURI = Path.of(Utils.ensureSuffix(basePath, "/")).toFile().toURI().toString();

        ArrayList<Path> paths = new ArrayList<>();
        for (String include : matchRules.includes) {
            try {
                nextResource:
                for (Resource resource : resolver.getResources(basePathURI + include)) {
                    File file = resource.getFile();

                    switch (fileType) {
                        case FileType.OnlyFiles:
                            if (!file.isFile())
                                continue;
                            break;
                        case FileType.OnlyDirectories:
                            if (!file.isDirectory())
                                continue;
                            break;
                        case FileType.FilesOrDirectories:
                            if (!file.isFile() && !file.isDirectory())
                                continue;
                            break;
                    }

                    Path path = file.toPath();
                    if (matchRules.hasExcludes()) {
                        String rest = path.toString().substring(basePathURI.length());
                        for (String exclude : matchRules.excludes) {
                            if (matcher.match(exclude, rest))
                                continue nextResource;
                        }
                    }
                    paths.add(path);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        paths.sort(Path::compareTo);
        return paths;
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        return nestedResourceIterator.hasNext();
    }

    @Override
    public Progress[] getProgress() {
        return new ProgressTracker[]{progressDocuments, progressFiles};
    }

    @Override
    public void getNext(JCas jCas) throws IOException, CollectionException {
        if (!nestedResourceIterator.hasNext()) {
            throw new CollectionException(new NoSuchElementException("No further documents to process!"));
        }

        Map.Entry<Path, List<Path>> entry = nestedResourceIterator.next();

        initCas(jCas, entry.getKey());

        long lastFileProgress = progressFiles.getCompleted();
        try {
            process(jCas, entry.getValue());
        } catch (IOException e) {
            if (stopAtFirstError) {
                throw e;
            } else {
                getLogger().warn("Caught IOException while processing XMLs of document {}; continuing as `stopAtFirstError` is set.", entry.getKey());
                getLogger().error("Ignored {} caused by {}", e, e.getCause().toString());

                // Remove further files of this document from progress total
                long completedOfThisDocument = progressFiles.getCompleted() - lastFileProgress;
                long missingForThisDocument = entry.getValue().size() - completedOfThisDocument;
                progressFiles.setTotal(progressFiles.getTotal() - missingForThisDocument);
            }
        }

        progressDocuments.inc();
        if (logFreq > 0) {
            getLogger().info("{}, {} - Finished Document: {}", progressDocuments, progressFiles, DocumentMetaData.get(jCas).getDocumentId());
        }
    }

    private void initCas(JCas jCas, Path rootPath) throws CollectionException {
        String rootName = rootPath.toFile().getName();
        String rootPathSuffix = rootPath.toString().substring(sourceLocation.length());
        if (rootPathSuffix.startsWith("/")) {
            rootPathSuffix = rootPathSuffix.substring(1);
        }
        String documentId = rootPathSuffix;
        if (documentIdPattern != null) {
            try {
                Matcher matcher = documentIdPattern.matcher(rootPathSuffix);
                if (matcher.matches()) {
                    for (int i = 1; i <= matcher.groupCount(); i++) {
                        if (matcher.group(i) != null) {
                            documentId = matcher.group(i);
                            break;
                        }
                    }
                } else {
                    throw new MatchException(
                            String.format("Document root path '%s' does not match document ID pattern '%s'", rootPathSuffix, documentIdPatternString),
                            null
                    );
                }
            } catch (Exception e) {
                throw new CollectionException(e);
            }
        }
        String documentUri = baseUri + documentId;

        // Set the DKPro Core document metadata
        DocumentMetaData docMetaData = DocumentMetaData.create(jCas);
        docMetaData.setDocumentTitle(rootName);
        docMetaData.setDocumentId(documentId);

        docMetaData.setDocumentBaseUri(baseUri);
        docMetaData.setDocumentUri(documentUri);

        if (!Strings.isNullOrEmpty(collectionId)) {
            docMetaData.setCollectionId(collectionId);
        }

        jCas.setDocumentLanguage(language);
    }

    public void process(JCas jCas, List<Path> files) throws IOException, CollectionException {
        FineReaderEventHandler.ParsedDocument parsedDocument = parseFiles(files);

        // Build SOFA string, remove HTML escapes
        String text = parsedDocument.tokens.stream().map(AbbyyToken::toString).collect(Collectors.joining(""));
        if (unescapeHTML) {
            text = StringEscapeUtils.unescapeHtml(text);
        }
        jCas.setDocumentText(text);
        jCas.setDocumentLanguage(language);

        Document ocrDocument = new Document(jCas);
        ocrDocument.setBegin(0);
        ocrDocument.setEnd(jCas.getDocumentText().length());
        jCas.addFsToIndexes(ocrDocument);

        // Add parsed elements to CAS
        if (addPages) {
            for (AbbyyPage page : parsedDocument.pages) {
                jCas.addFsToIndexes(page.into(jCas));
            }
        }
        if (addBlocks) {
            for (AbbyyBlock block : parsedDocument.blocks) {
                jCas.addFsToIndexes(block.into(jCas));
            }
        }
        if (addParagraphs) {
            for (AbbyyParagraph paragraph : parsedDocument.paragraphs) {
                jCas.addFsToIndexes(paragraph.into(jCas));
            }
        }
        if (addLines) {
            for (AbbyyLine line : parsedDocument.lines) {
                if (!addLineFormat) {
                    line.setFormat(null);
                }
                Line ocrLine = (Line) line.into(jCas);
                jCas.addFsToIndexes(ocrLine);

                if (enableGarbageHeuristics) {
                    detectAnomaly(jCas, ocrLine);
                }
            }
        }
        if (addTokens) {
            for (AbbyyToken token : parsedDocument.tokens) {
                if (!token.isSpace()) {
                    jCas.addFsToIndexes(token.into(jCas));
                }
            }
        }
    }

    private FineReaderEventHandler.ParsedDocument parseFiles(List<Path> files) throws IOException, CollectionException {
        FineReaderEventHandler fineReaderEventHandler = new FineReaderEventHandler();
        fineReaderEventHandler.setUnifyWhitespaces(unifyWhitespaces);
        fineReaderEventHandler.setBoundingBox(boundingBox);

        for (Path path : files) {
            String fileName = path.toFile().getName();

            String pageIndex;
            String pageId;

            Matcher matcher = pageIdPattern.matcher(fileName);
            Map<String, Integer> namedGroups = matcher.namedGroups();
            if (matcher.matches() && namedGroups.containsKey("index") && namedGroups.containsKey("id")) {
                pageIndex = matcher.group(namedGroups.getOrDefault("index", 1));
                pageId = matcher.group(namedGroups.getOrDefault("id", 2));
            } else {
                throw new CollectionException(new MatchException(
                        String.format("Name of file '%s' does not match page index/ID pattern: '%s'", path, pageIdPatternString),
                        null
                ));
            }

            try {
                fineReaderEventHandler.setNextPageIndex(Integer.parseInt(pageIndex));
            } catch (NumberFormatException e) {
                getLogger().warn("Failed to parse page index '{}' as Integer for page {}: {}", pageIndex, path, e.getMessage());
            }

            fineReaderEventHandler.setNextPageId(pageId);
            fineReaderEventHandler.setNextPageUri(baseUri + pageId);

            try {
                saxParser.parse(openInputStream(path), fineReaderEventHandler);
            } catch (SAXException e) {
                throw new IOException(path.toString(), e);
            }
            progressFiles.inc();
            if (logFreq > 0 && progressFiles.getCompleted() % logFreq == 0) {
                getLogger().info("{}, {}", progressDocuments, progressFiles);
            }
        }

        return fineReaderEventHandler.getParsedDocument();
    }

    private static InputStream openInputStream(Path path) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(path.toFile());
        String pathString = path.toString();
        try {
            if (pathString.endsWith(".gz")) {
                return new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP, fileInputStream);
            } else if (pathString.endsWith(".bz2")) {
                return new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.BZIP2, fileInputStream);
            } else if (pathString.endsWith(".xz")) {
                return new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.XZ, fileInputStream);
            } else {
                return fileInputStream;
            }
        } catch (CompressorException impossible) {
            throw new UIMARuntimeException(impossible);
        }
    }

    private void detectAnomaly(JCas jCas, Annotation annotation) {
        try {
            final String coveredText = annotation.getCoveredText();

            final boolean isNumberTable = Patterns.weirdNumberTable.matcher(coveredText).matches();
            boolean isGarbageLine = !isNumberTable || Patterns.yearPattern.matcher(coveredText).matches();

            final boolean isLetterTable = Patterns.weirdLetterTable.matcher(coveredText).matches();
            isGarbageLine &= !isLetterTable;

            if (!isGarbageLine) {
                String description = String.format("weirdNumberTable:%b, weirdLetterTable:%b", isNumberTable, isLetterTable);
                tagAnomaly(jCas, description, annotation.getBegin(), annotation.getEnd(), "ABBYY:" + annotation.getType().getName(), "");
            }
        } catch (Exception e) {
            getLogger().error(e.toString());
        }
    }

    private void tagAnomaly(JCas jCas, String description, int begin, int end, String anomalyType, String replacement) {
        Anomaly anomaly = new Anomaly(jCas, begin, end);
        anomaly.setCategory(anomalyType);
        anomaly.setDescription(description);
        SuggestedAction suggestedAction = new SuggestedAction(jCas);
        suggestedAction.setReplacement(replacement);
        FSArray<SuggestedAction> fsArray = new FSArray<>(jCas, 1);
        fsArray.set(0, suggestedAction);
        anomaly.setSuggestions(fsArray);
        jCas.addFsToIndexes(anomaly);
    }
}

package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.uima.jcas.JCas;
import org.hucompute.textimager.uima.type.WikipediaInformation;
import org.hucompute.textimager.uima.type.wikipedia.WikipediaLink;
import org.javaync.io.AsyncFiles;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.ByteReadFuture;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class DUUIWikipediaExtractorReader extends DUUIFileReaderLazy {

    public DUUIWikipediaExtractorReader(String folder, String ending) {
        super(folder, ending, 10, -1, null, "", false, null, 0);
    }

    public DUUIWikipediaExtractorReader(String folder, String ending, int iDebug) {
        super(folder, ending, iDebug, -1, null, "", false, null, 0);
    }

    public DUUIWikipediaExtractorReader(String folder, String ending, int iDebug, String sLanguage) {
        super(folder, ending, iDebug, -1, null, "", false, sLanguage, 0);
    }

    public DUUIWikipediaExtractorReader(String folder, String ending, int iDebug, String sLanguage, String sOutputPath, String sTargetEnding) {
        super(folder, ending, iDebug, -1, null, "", false, sLanguage, 0);
        this._targetPath = sOutputPath;
        this._targetEnding = sTargetEnding;
    }

    public static String getSize(String sPath) {
        return FileUtils.byteCountToDisplaySize(new File(sPath).length());
    }

    private void splitAndLoad() throws IOException {
        if (_filePaths.size() > 0 && _loadedFiles.size() < debugCount * 10) {
            String result = _filePaths.poll();

            String sContent = org.texttechnologylab.utilities.helper.FileUtils.getContentFromFile(new File(result));
            String[] sSplit = sContent.split("\n");
            _collectionSize.addAndGet(sSplit.length);
            for (String s : sSplit) {
                _loadedFiles.add(new ByteReadFuture(result, s.getBytes()));
            }

        }

    }

    @Override
    public void getNextCas(JCas empty) {

        boolean bFoundText = false;
        JSONObject pObject = new JSONObject();
        File f = null;
        File fCheck = null;

        do {
            bFoundText = false;
            fCheck = null;

            try {
                splitAndLoad();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ByteReadFuture future = _loadedFiles.poll();

            byte[] file = null;
            String result = null;

            if (future == null) {
                return;
            }
            result = future.getPath();
            file = future.getBytes();

            f = new File(result);

            long factor = 1;
            _currentMemorySize.getAndAdd(-factor * (long) file.length);

            int val = _docNumber.addAndGet(1);

            progress.setDone(val);
            progress.setMax(_collectionSize.get());
            progress.setLeft(_collectionSize.get() - val);

            if (_collectionSize.get() - progress.getCount() > debugCount) {
                if (val % debugCount == 0 || val == 0) {
                    System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
                }
            } else {
                System.out.printf("%s: \t %s \t %s\n", progress, getSize(result), result);
            }

            String sContent = new String(file);

            pObject = new JSONObject(sContent);
            if (pObject.getString("text").length() == 0) {
                bFoundText = true;
            }

            if (this._targetPath.length() > 0 && this._targetEnding.length() > 0) {
                fCheck = new File(this._targetPath + f.toPath().getParent().getFileName().toString() + "/" + pObject.getString("id") + "." + this._targetEnding);
            }

        }
        while (bFoundText || (fCheck != null && fCheck.exists()));


        Document pDocument = Jsoup.parse(StringEscapeUtils.unescapeHtml(pObject.getString("text")));

        StringBuilder sb = new StringBuilder();

        List<Node> nl = pDocument.select("body").get(0).childNodes();
        for (int a = 0; a < nl.size(); a++) {
            Node n = nl.get(a);
            switch (n.nodeName()) {
                case "#text":
                    sb.append(((TextNode) n).text());
                    break;

                case "a":
                    String sValue = ((Element) n).text();
                    String sLink = n.attr("href");
                    WikipediaLink nLink = new WikipediaLink(empty);
                    nLink.setTarget(sLink);
                    nLink.setLinkType("internal");
                    nLink.setBegin(sb.length());
                    nLink.setEnd(sb.length() + sValue.length());
                    nLink.addToIndexes();
                    sb.append(sValue);
                    break;
            }
        }

        empty.setDocumentText(sb.toString());

        DocumentMetaData dmd = DocumentMetaData.create(empty);
        dmd.setDocumentId(pObject.getString("id"));
        dmd.setDocumentTitle(pObject.getString("title"));
        dmd.setDocumentUri("/opt/wikipedia/" + f.toPath().getParent().getFileName().toString() + "/" + pObject.getString("id"));
        dmd.setDocumentBaseUri("/opt/wikipedia/");
        dmd.addToIndexes();


        WikipediaInformation wi = new WikipediaInformation(empty);
        wi.setBegin(0);
        wi.setEnd(sb.toString().length());
        wi.setNamespace("ns0");
        wi.setPageID(pObject.getString("id"));
        wi.setRevisionID(pObject.getString("revid"));
        wi.setPageURL(pObject.getString("url"));
        wi.addToIndexes();

        if (_language != null && !_language.isEmpty()) {
            empty.setDocumentLanguage(_language);
        }

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

}

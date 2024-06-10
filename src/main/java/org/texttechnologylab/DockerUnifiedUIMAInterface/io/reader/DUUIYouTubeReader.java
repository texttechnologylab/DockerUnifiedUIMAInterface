package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.io.FileUtils;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.FSList;
import org.json.JSONArray;
import org.json.JSONObject;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring.AdvancedProgressMeter;
import org.texttechnologylab.annotation.socialmedia.metadata.YouTube;
import org.texttechnologylab.annotation.socialmedia.metadata.youtube.Playlist;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DUUIYouTubeReader implements DUUICollectionReader {

    private String _path;
    private ConcurrentLinkedQueue<String> _filePaths;
    private ConcurrentLinkedQueue<String> _filePathsBackup;
    private ConcurrentLinkedQueue<ByteReadFuture> _loadedFiles;

    private String _viewName;

    private int _initialSize;
    private AtomicInteger _docNumber;
    private long _maxMemory;
    private AtomicLong _currentMemorySize;

    private boolean _addMetadata = true;

    private String _language = null;

    private AdvancedProgressMeter progress = null;

    private int debugCount = 25;

    private Map<String, List<String>> _videosPlaylists;
    private String _apiKey;

    public DUUIYouTubeReader(String youtubeLink, String apiKey) throws IOException, InterruptedException {
        this(youtubeLink, apiKey, "_InitialView", 25, getRandomFromMode(null, -1), false, null);
    }

    public DUUIYouTubeReader(String youtubeLink, String apiKey, String viewName) throws IOException, InterruptedException {
        this(youtubeLink, apiKey, viewName, 25, getRandomFromMode(null, -1), false, null);
    }

    public DUUIYouTubeReader(String youtubeLink, String apiKey, String viewName, int debugCount, int iRandom, boolean bAddMetadata, String language) throws IOException, InterruptedException {
        _addMetadata = bAddMetadata;
        _language = language;
        _filePaths = new ConcurrentLinkedQueue<>();
        _loadedFiles = new ConcurrentLinkedQueue<>();
        _filePathsBackup = new ConcurrentLinkedQueue<>();
        _videosPlaylists = new HashMap<>();
        _apiKey = apiKey;
        _viewName = viewName;

        if(youtubeLink.contains("&list=")) {  // Is playlist

            String[] parameters = youtubeLink.split("&");

            String playlistId = "";
            for (String parameter : parameters) {
                if (parameter.startsWith("list=")) {
                    playlistId = parameter.substring(5);
                    break;
                }
            }

            try {
                JSONObject jsonObject = getPlaylistVideos(playlistId);

                JSONArray jsonItems = jsonObject.getJSONArray("items");

                for (int i = 0; i < jsonItems.length(); i++) {
                    String videoId = jsonItems.getJSONObject(i).getJSONObject("contentDetails").getString("videoId");
                    _filePaths.add("https://www.youtube.com/watch?v=" + videoId);

                    _videosPlaylists.put(videoId, Arrays.asList(playlistId));
                }

            } catch (Exception e) {
                throw e;
            }
        }else if(youtubeLink.contains("watch?v")){  // Is single video
            _filePaths.add(youtubeLink);
        }else{  // Is Channel

            String pageToken = "";
            String channelId = null;

            if(youtubeLink.contains("/@")){
                channelId = getChannelIdByHandle(youtubeLink.split("@")[1].split("/")[0]);
            }
            else if(youtubeLink.contains("/channel/")){
                channelId = youtubeLink.split("/channel/")[1].split("/")[0];
            }

            if(channelId != null){

                do{
                    JSONObject jsonObject = getChannelVideosByChannelId(channelId, "");

                    JSONArray jsonItems = jsonObject.getJSONArray("items");

                    for (int i = 0; i < jsonItems.length(); i++) {
                        JSONObject idRequestObject = jsonItems.getJSONObject(i).getJSONObject("id");

                        if(!idRequestObject.has("videoId")) continue;  // Found own channel instead of video

                        String videoId = idRequestObject.getString("videoId");
                        _filePaths.add("https://www.youtube.com/watch?v=" + videoId);
                        System.out.println("Added video: " + i);
                    }

                    if(jsonObject.has("nextPageToken"))
                        pageToken = jsonObject.getString("nextPageToken");
                    else
                        pageToken = "";

                }while(!pageToken.equals(""));

            }

        }

        if (iRandom > 0) {
            _filePaths = random(_filePaths, iRandom);
        }

        _filePathsBackup.addAll(_filePaths);

        this.debugCount = debugCount;

        System.out.printf("Found %d files matching the pattern! \t Using Random: %d\n", _filePaths.size(), iRandom);
        _initialSize = _filePaths.size();
        _docNumber = new AtomicInteger(0);
        _currentMemorySize = new AtomicLong(0);
        // 500 MB
        _maxMemory = 500 * 1024 * 1024;

        progress = new AdvancedProgressMeter(_initialSize);
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

    public static void addFilesToConcurrentList(File folder, String ending, ConcurrentLinkedQueue<String> paths) {
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                if (listOfFiles[i].getName().endsWith(ending)) {
                    paths.add(listOfFiles[i].getPath().toString());
                }
            } else if (listOfFiles[i].isDirectory()) {
                addFilesToConcurrentList(listOfFiles[i], ending, paths);
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

    @Override
    public void getNextCas(JCas empty) {
        ByteReadFuture future = _loadedFiles.poll();

        String result = null;
        if (future == null) {
            result = _filePaths.poll();
            if (result == null) return;
        } else {
            result = future.getPath();
            long factor = 1;
            if (result.endsWith(".gz") || result.endsWith(".xz")) {
                factor = 10;
            }
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

        try {
            empty.reset();

            JCas ytView;
            try {
                ytView = empty.getView(_viewName);

            }catch (Exception e){
                ytView = empty.createView(_viewName);
            }

            String videoId = result.split("v=")[1].split("&list=")[0];
            ytView.setSofaDataString("https://www.youtube.com/watch?v=" + videoId, "text/x-uri");

            if(_addMetadata)
                setVideoMetadata(videoId, ytView);

        } catch (Exception e) {
            e.printStackTrace();
        }


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

    }

    public void reset() {
        _filePaths = _filePathsBackup;
        _docNumber.set(0);
        progress = new AdvancedProgressMeter(_initialSize);
    }

    @Override
    public boolean hasNext() {
        return _filePaths.size() > 0;
    }

    @Override
    public long getSize() {
        return _filePaths.size();
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

    private JSONObject getPlaylistVideos(String playlistId) throws IOException, InterruptedException {
        String url = "https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=" + playlistId + "&key=" + _apiKey;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());

        return new JSONObject(response.body().toString());
    }

    private String getChannelIdByHandle(String channelHandle) throws IOException, InterruptedException {
        String url = "https://youtube.googleapis.com/youtube/v3/search?part=snippet&maxResults=1&q=" + channelHandle + "&type=channel&key=" + _apiKey;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JSONObject jsonObject = new JSONObject(response.body().toString());
        JSONArray resultArray = jsonObject.getJSONArray("items");

        if(resultArray.length() == 0) return null;

        return resultArray.getJSONObject(0).getJSONObject("id").getString("channelId");
    }

    private JSONObject getChannelVideosByChannelId(String channelId, String pageToken) throws IOException, InterruptedException {
        String url = "https://www.googleapis.com/youtube/v3/search?key=" + _apiKey + "&channelId=" + channelId +"&part=id&order=date&maxResults=50&pageToken=" + pageToken;

        System.out.println(url);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());

        return new JSONObject(response.body().toString());
    }

    private void setVideoMetadata(String id, JCas jCas) throws IOException, InterruptedException{
        // TODO: set playlist and youtube url in metadata

        String url = "https://youtube.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics%2CcontentDetails&id=" + id + "&key=" + _apiKey;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());


        JSONObject jsonObject = new JSONObject(response.body().toString());

        JSONObject item = jsonObject.getJSONArray("items").getJSONObject(0);

        JSONObject snippet = item.getJSONObject("snippet");
        JSONObject statistics = item.getJSONObject("statistics");
        JSONObject contentDetails = item.getJSONObject("contentDetails");


        YouTube youTube = new YouTube(jCas);

        if(_videosPlaylists.containsKey(id)){
            List<String> playlistIds = _videosPlaylists.get(id);
            Playlist[] playlists = new Playlist[_videosPlaylists.get(id).size()];

            for (int i = 0; i < playlistIds.size(); i++){
                Playlist playlist = new Playlist(jCas);

                String playlistUrl = "https://www.googleapis.com/youtube/v3/playlists?part=snippet&id=" + playlistIds.get(i) + "&key=" + _apiKey;
                request = HttpRequest.newBuilder()
                        .uri(URI.create(playlistUrl))
                        .build();

                response = client.send(request, HttpResponse.BodyHandlers.ofString());


                JSONObject playlistJsonObject = new JSONObject(response.body().toString());
                JSONObject playlistItem = playlistJsonObject.getJSONArray("items").getJSONObject(0);
                JSONObject playlistSnippet = playlistItem.getJSONObject("snippet");

                playlist.setName(playlistSnippet.getString("title"));
                playlist.setCreateDate(youtubeDateToInt(playlistSnippet.getString("publishedAt")));
                playlist.setUrl("https://www.youtube.com/watch?v=" + id + "&list=" + playlistIds.get(i));

                playlists[i] = playlist;
            }

            FSList<Playlist> list = FSList.create(jCas, playlists);
            list.addToIndexes();
        }

        youTube.setUrl("https://www.youtube.com/watch?v=" + id);
        youTube.setChannelName(snippet.getString("channelTitle"));
        youTube.setChannelURL("https://www.youtube.com/channel/" + snippet.getString("channelId"));
        String sDuration = contentDetails.getString("duration").substring(2);
        int iDuration = 0;

        if(sDuration.contains("H")){
            String[] hours = sDuration.split("H");
            String[] minutes = hours[1].split("M");
            String seconds = minutes[1].split("S")[0];

            iDuration = Integer.parseInt(hours[0]) * 360 + Integer.parseInt(minutes[0]) * 60 + Integer.parseInt(seconds);
        }else if(sDuration.contains("M")){
            String[] minutes = sDuration.split("M");
            String seconds = minutes[1].split("S")[0];

            iDuration = Integer.parseInt(minutes[0]) * 60 + Integer.parseInt(seconds);
        }else if(sDuration.contains("S")){
            String seconds = sDuration.split("S")[0];

            iDuration = Integer.parseInt(seconds);
        }

        youTube.setLength(iDuration);
        youTube.setViews(Integer.parseInt(statistics.getString("viewCount")));
        youTube.setLikes(Integer.parseInt(statistics.getString("likeCount")));
        youTube.setDislikes(0);  // Does not support dislikes

        youTube.setCreateDate(youtubeDateToInt(snippet.getString("publishedAt")));

        youTube.setDownloadDate(currentDateToInt());
        youTube.addToIndexes();
    }

    private int youtubeDateToInt(String youtubeDate){
        String[] dateElements = youtubeDate.split("T")[0].split("-");  // Seperate date and time

        if(dateElements[0].length() == 1)
            dateElements[0] = "0" + dateElements[0];

        if(dateElements[1].length() == 1)
            dateElements[1] = "0" + dateElements[1];

        int iCreateDate = Integer.parseInt(dateElements[2] + dateElements[1] + dateElements[0]);
        return iCreateDate;
    }

    private int currentDateToInt(){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");
        return Integer.parseInt(ZonedDateTime.now().format(formatter));
    }

    private String readFile(File file) throws FileNotFoundException {
        String result = "";
        Scanner myReader = new Scanner(file);
        while (myReader.hasNextLine()) {
            if(result == ""){
                result = myReader.nextLine();
            }else{
                result += "\n" + myReader.nextLine();
            }
        }

        return result;
    }

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
}
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
    private ConcurrentLinkedQueue<YouTubeVideo> _youtubeVideos;
    private ConcurrentLinkedQueue<YouTubeVideo> _youtubeVideosBackup;

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
        this(youtubeLink, apiKey, "_InitialView", 25, getRandomFromMode(null, -1), true, null);
    }

    public DUUIYouTubeReader(String youtubeLink, String apiKey, String viewName) throws IOException, InterruptedException {
        this(youtubeLink, apiKey, viewName, 25, getRandomFromMode(null, -1), true, null);
    }

    public DUUIYouTubeReader(String youtubeLink, String apiKey, String viewName, int debugCount, int iRandom, boolean bAddMetadata, String language) throws IOException, InterruptedException {
        _addMetadata = bAddMetadata;
        _language = language;
        _youtubeVideos = new ConcurrentLinkedQueue<>();
        _youtubeVideosBackup = new ConcurrentLinkedQueue<>();
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
                String pageToken = "";

                do{
                    List<YouTubeVideo> pagedVideos = new LinkedList<>();

                    JSONObject jsonObject = getPlaylistVideos(playlistId, pageToken);

                    JSONArray jsonItems = jsonObject.getJSONArray("items");

                    for (int i = 0; i < jsonItems.length(); i++) {
                        String videoId = jsonItems.getJSONObject(i).getJSONObject("contentDetails").getString("videoId");
                        pagedVideos.add(new YouTubeVideo(videoId));

                        _videosPlaylists.put(videoId, Arrays.asList(playlistId));
                    }

                    if(_addMetadata){
                        generateBulkMetadata(pagedVideos);
                    }

                    if(jsonObject.has("nextPageToken"))
                        pageToken = jsonObject.getString("nextPageToken");
                    else
                        pageToken = "";

                    _youtubeVideos.addAll(pagedVideos);
                }while(!pageToken.equals(""));

            } catch (Exception e) {
                throw e;
            }
        }else if(youtubeLink.contains("watch?v")) {  // Is single video
            youtubeLink = youtubeLink.split("watch\\?v=")[1].split("&")[0];

            YouTubeVideo video = new YouTubeVideo(youtubeLink);
            _youtubeVideos.add(video);

            if(_addMetadata){
                generateMetadata(video);
            }
        }else if(youtubeLink.contains("youtu.be/")){  // Is single video with shortened url
            youtubeLink = youtubeLink.split("youtu.be/")[1].split("&")[0];

            YouTubeVideo video = new YouTubeVideo(youtubeLink);
            _youtubeVideos.add(video);

            if(_addMetadata){
                generateMetadata(video);
            }
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
                    List<YouTubeVideo> pagedVideos = new LinkedList<>();

                    JSONObject jsonObject = getChannelVideosByChannelId(channelId, "");

                    JSONArray jsonItems = jsonObject.getJSONArray("items");

                    for (int i = 0; i < jsonItems.length(); i++) {
                        JSONObject idRequestObject = jsonItems.getJSONObject(i).getJSONObject("id");

                        if(!idRequestObject.has("videoId")) continue;  // Found own channel instead of video

                        String videoId = idRequestObject.getString("videoId");
                        pagedVideos.add(new YouTubeVideo(videoId));
                        System.out.println("Added video: " + i);
                    }

                    if(_addMetadata){
                        generateBulkMetadata(pagedVideos);
                    }

                    if(jsonObject.has("nextPageToken"))
                        pageToken = jsonObject.getString("nextPageToken");
                    else
                        pageToken = "";

                    _youtubeVideos.addAll(pagedVideos);

                }while(!pageToken.equals(""));

            }

        }

        if (iRandom > 0) {
            _youtubeVideos = random(_youtubeVideos, iRandom);
        }

        _youtubeVideosBackup.addAll(_youtubeVideos);

        this.debugCount = debugCount;

        System.out.printf("Found %d files matching the pattern! \t Using Random: %d\n", _youtubeVideos.size(), iRandom);
        _initialSize = _youtubeVideos.size();
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

    public static ConcurrentLinkedQueue<YouTubeVideo> random(ConcurrentLinkedQueue<YouTubeVideo> videos, int iRandom) {

        ConcurrentLinkedQueue<YouTubeVideo> rQueue = new ConcurrentLinkedQueue<YouTubeVideo>();

        Random nRandom = new Random(iRandom);

        ArrayList<YouTubeVideo> sList = new ArrayList<>();
        sList.addAll(videos);

        Collections.shuffle(sList, nRandom);

        if (iRandom > sList.size()) {
            rQueue.addAll(sList.subList(0, sList.size()));
        } else {
            rQueue.addAll(sList.subList(0, iRandom));
        }


        return rQueue;

    }

    @Override
    public AdvancedProgressMeter getProgress() {
        return this.progress;
    }

    @Override
    public void getNextCas(JCas empty) {

        YouTubeVideo result = _youtubeVideos.poll();

        int val = _docNumber.addAndGet(1);

        progress.setDone(val);
        progress.setLeft(_initialSize - val);

        if (_initialSize - progress.getCount() > debugCount) {
            if (val % debugCount == 0 || val == 0) {
                System.out.printf("%s \t %s\n", progress, result.getVideoUrl());
            }
        } else {
            System.out.printf("%s \t %s\n", progress, result.getVideoUrl());
        }

        try {
            empty.reset();

            JCas ytView;
            try {
                ytView = empty.getView(_viewName);

            }catch (Exception e){
                ytView = empty.createView(_viewName);
            }

            ytView.setSofaDataString(result.getVideoUrl(), "text/x-uri");

            if(_addMetadata)
                setVideoMetadata(result, ytView);

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (_addMetadata) {
            if (JCasUtil.select(empty, DocumentMetaData.class).size() == 0) {
                DocumentMetaData dmd = DocumentMetaData.create(empty);
                dmd.setDocumentId(result._id);
                dmd.setDocumentTitle(result._title);
                //dmd.setDocumentUri(result.getVideoUrl());
                dmd.addToIndexes();
            }
        }

        if (_language != null && !_language.isEmpty()) {
            empty.setDocumentLanguage(_language);
        }

    }

    public void reset() {
        _youtubeVideos = _youtubeVideosBackup;
        _docNumber.set(0);
        progress = new AdvancedProgressMeter(_initialSize);
    }

    @Override
    public boolean hasNext() {
        return _youtubeVideos.size() > 0;
    }

    @Override
    public long getSize() {
        return _youtubeVideos.size();
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

    private JSONObject getPlaylistVideos(String playlistId, String pageToken) throws IOException, InterruptedException {
        String url = "https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId=" + playlistId + "&key=" + _apiKey + "&maxResults=50&pageToken=" + pageToken;
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

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());

        return new JSONObject(response.body().toString());
    }

    private void setVideoMetadata(YouTubeVideo video, JCas jCas) throws IOException, InterruptedException{

        YouTube youTube = new YouTube(jCas);

        if(_videosPlaylists.containsKey(video.getVideoId())){
            List<String> playlistIds = _videosPlaylists.get(video.getVideoId());
            Playlist[] playlists = new Playlist[_videosPlaylists.get(video.getVideoId()).size()];

            for (int i = 0; i < playlistIds.size(); i++){
                Playlist playlist = new Playlist(jCas);

                String playlistUrl = "https://www.googleapis.com/youtube/v3/playlists?part=snippet&id=" + playlistIds.get(i) + "&key=" + _apiKey;

                HttpClient client = HttpClient.newHttpClient();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(playlistUrl))
                        .build();

                HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());


                JSONObject playlistJsonObject = new JSONObject(response.body().toString());
                JSONObject playlistItem = playlistJsonObject.getJSONArray("items").getJSONObject(0);
                JSONObject playlistSnippet = playlistItem.getJSONObject("snippet");

                playlist.setName(playlistSnippet.getString("title"));
                playlist.setCreateDate(youtubeDateToInt(playlistSnippet.getString("publishedAt")));
                playlist.setUrl("https://www.youtube.com/watch?v=" + video.getVideoId() + "&list=" + playlistIds.get(i));

                playlists[i] = playlist;
            }

            FSList<Playlist> list = FSList.create(jCas, playlists);
            list.addToIndexes();

            youTube.setPlaylist(list);
        }

        youTube.setName(video._title);
        youTube.setUrl(video.getVideoUrl());
        youTube.setChannelName(video._channelName);
        youTube.setChannelURL(video._channelUrl);
        youTube.setLength(video._duration);
        youTube.setViews(video._views);
        youTube.setLikes(video._likes);
        youTube.setDislikes(0);  // Does not support dislikes
        youTube.setCreateDate(video._createDate);

        youTube.setDownloadDate(currentDateToInt());
        youTube.addToIndexes();
    }

    private void generateMetadata(YouTubeVideo video) throws IOException, InterruptedException {
        List<YouTubeVideo> videos = new LinkedList<>();
        videos.add(video);
        generateBulkMetadata(videos);
    }

    private void generateBulkMetadata(List<YouTubeVideo> videos) throws IOException, InterruptedException {
        if(videos.isEmpty()) return;

        String ids = "";

        for(YouTubeVideo video : videos){
            if(ids.equals("")){
                ids = video.getVideoId();
            }else{
                ids += "," + video.getVideoId();
            }
        }


        String url = "https://youtube.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics%2CcontentDetails&id=" + ids + "&key=" + _apiKey;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JSONObject jsonObject = new JSONObject(response.body().toString());

        JSONArray items = jsonObject.getJSONArray("items");

        for(int i = 0; i < items.length(); i++){
            JSONObject snippet = items.getJSONObject(i).getJSONObject("snippet");
            JSONObject statistics = items.getJSONObject(i).getJSONObject("statistics");
            JSONObject contentDetails = items.getJSONObject(i).getJSONObject("contentDetails");

            videos.get(i).setMetadata(snippet, statistics, contentDetails);
        }
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

    class YouTubeVideo{
        private String _id;
        private String _channelName;
        private String _channelUrl;
        private String _title;
        private int _duration;
        private int _views;
        private int _likes;
        private int _createDate;

        public YouTubeVideo(String id){
            _id = id;
        }

        public String getVideoId(){
            return _id;
        }

        public String getVideoUrl(){
            return "https://www.youtube.com/watch?v=" + _id;
        }

        public void setMetadata(JSONObject snippet, JSONObject statistics, JSONObject contentDetails){

            _title = snippet.getString("title");
            _channelName = snippet.getString("channelTitle");
            _channelUrl = "https://www.youtube.com/channel/" + snippet.getString("channelId");
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

            _duration = iDuration;
            _views = Integer.parseInt(statistics.getString("viewCount"));
            _likes = Integer.parseInt(statistics.getString("likeCount"));

            _createDate = youtubeDateToInt(snippet.getString("publishedAt"));
        }
    }
}
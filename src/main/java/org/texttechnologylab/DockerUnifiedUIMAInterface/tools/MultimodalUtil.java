package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.commons.codec.binary.Base64;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.type.AudioToken;

import java.io.*;
import java.util.*;

public class MultimodalUtil {

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends AudioToken> List<File> getAllCoveredAudio(JCas audioTokenView, Class<T> audioToken) throws CASException {
        return getAllCoveredAudio(audioTokenView, audioToken, null, "wav");
    }

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @param targetFormat File format for the output file. (like "wav" or "mp3")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends AudioToken> List<File> getAllCoveredAudio(JCas audioTokenView, Class<T> audioToken, String targetFormat) throws CASException {
        return getAllCoveredAudio(audioTokenView, audioToken, null, targetFormat);
    }

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @param audioFileView The view containing the entire audio file in its sofa string (If null, tries to auto-detect)
     * @param targetFormat File format for the output file. (like "wav" or "mp3")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends AudioToken> List<File> getAllCoveredAudio(JCas audioTokenView, Class<T> audioToken, JCas audioFileView, String targetFormat) throws CASException {

        List<File> files = new ArrayList<>();
        List<String> commands = new LinkedList<>();

        JCasUtil.select(audioTokenView, audioToken).forEach(token -> {
            commands.add(String.format("-ss %s -t %s %s",
                    token.getTimeStart(),
                    token.getTimeEnd() - token.getTimeStart(),
                    getOutputName(token, targetFormat)));


            File file = new File(getOutputName(token, targetFormat));
            file.deleteOnExit();
            files.add(file);
        });

        MultimodalUtil.getEveryAudioSegment(audioTokenView, audioFileView, commands);

        return files;
    }

    /**
     * Converts a AudioToken into its own audio snippet
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @return A file containing the audio segment
     * @throws CASException
     */
    public static File getCoveredAudio(JCas audioTokenView, AudioToken audioToken) throws CASException {
        return getCoveredAudio(audioTokenView, audioToken, null, "wav");
    }

    /**
     * Converts a AudioToken into its own audio snippet
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @param targetFormat File format for the output file. (like "wav" or "mp3")
     * @return A file containing the audio segment
     * @throws CASException
     */
    public static File getCoveredAudio(JCas audioTokenView, AudioToken audioToken, String targetFormat) throws CASException {
        return getCoveredAudio(audioTokenView, audioToken, null, targetFormat);
    }

    /**
     * Converts a AudioToken into its own audio snippet
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @param audioFileView The view containing the entire audio file in its sofa string (If null, tries to auto-detect)
     * @param targetFormat File format for the output file. (like "wav" or "mp3")
     * @return A file containing the audio segment
     * @throws CASException
     */
    public static File getCoveredAudio(JCas audioTokenView, AudioToken audioToken, JCas audioFileView, String targetFormat) throws CASException {

        if(audioFileView == null)
            audioFileView = predictAudioView(audioTokenView);

        String inputFileName = "temp_" + audioFileView.getViewName();
        String outputFileName = getOutputName(audioToken, targetFormat);

        if(!new File(inputFileName).exists()) {
            // Convert encoded string to file
            OutputStream stream = null;
            try {
                stream = new FileOutputStream(inputFileName);
                stream.write(Base64.decodeBase64(audioFileView.getSofaDataString()));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        File inputFile = new File(inputFileName);
        inputFile.deleteOnExit();

        executeFFMpeg(inputFile.getAbsolutePath(), outputFileName, audioToken.getTimeStart(), audioToken.getTimeEnd());

        File outputFile = new File(outputFileName);
        outputFile.deleteOnExit();

        return outputFile;
    }

    private static void getEveryAudioSegment(JCas audioTokenCas, JCas audioFileView, List<String> commands) throws CASException {

        if(audioFileView == null)
            audioFileView = predictAudioView(audioTokenCas);

        String inputFileName = "temp_" + audioFileView.getViewName();

        if(!new File(inputFileName).exists()) {
            // Convert encoded string to file
            OutputStream stream = null;
            try {
                stream = new FileOutputStream(inputFileName);
                stream.write(Base64.decodeBase64(audioTokenCas.getSofaDataString()));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

        File inputFile = new File(inputFileName);
        inputFile.deleteOnExit();

        executeFFMpeg(inputFile.getAbsolutePath(), commands);
    }

    /**
     * Tries to find the view which contains the entire audio file in its sofa string
     * @param fromJCas JCas to search the views in
     * @throws CASException
     */
    public static JCas predictAudioView(JCas fromJCas) throws CASException {
        Iterator<JCas> iter = fromJCas.getViewIterator();

        while(iter.hasNext()){
            JCas view = iter.next();

            if(view.getSofaMimeType().startsWith("audio/")){
                return view;
            }
        }

        return fromJCas;
    }


    /**
     * Converts each AudioToken into its own video snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @return List of files, each file containing the video content.
     * @throws CASException
     */
    public static <T extends AudioToken>List<File> getAllCoveredVideo(JCas audioTokenView, Class<T> audioToken) throws CASException {
        return getAllCoveredVideo(null, audioTokenView, audioToken);
    }

    /**
     * Converts each AudioToken into its own audio snippet.
     * @param videoFileView The view containing the entire video file in its sofa string (If null, tries to auto-detect)
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends AudioToken>List<File> getAllCoveredVideo(JCas videoFileView, JCas audioTokenView, Class<T> audioToken) throws CASException {
        return getAllCoveredVideo(videoFileView, audioTokenView, audioToken, "mp4");
    }

    /**
     * Converts each AudioToken into its own audio snippet.
     * @param videoFileView The view containing the entire video file in its sofa string (If null, tries to auto-detect)
     * @param audioTokenView The view in which the audio tokens are stored
     * @param audioToken The audio token class (like AudioToken)
     * @param targetFormat File format for the output file. (like "mp4" or "webm")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends AudioToken>List<File> getAllCoveredVideo(JCas videoFileView, JCas audioTokenView, Class<T> audioToken, String targetFormat) throws CASException {

        if(videoFileView == null){
            videoFileView = predictVideoView(audioTokenView);
        }

        List<File> files = new ArrayList<>();
        List<String> commands = new LinkedList<>();

        JCasUtil.select(audioTokenView, audioToken).forEach(token -> {
            commands.add(String.format("-ss %s -t %s %s",
                    token.getTimeStart(),
                    token.getTimeEnd() - token.getTimeStart(),
                    getOutputName(token, targetFormat)));


            File file = new File(getOutputName(token, targetFormat));
            file.deleteOnExit();
            files.add(file);
        });

        MultimodalUtil.getEveryVideoSegment(videoFileView, commands);

        return files;
    }


    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param videoFileView The view containing the entire video file in its sofa string (If null, tries to auto-detect)
     * @param audioToken The audio token class (like AudioToken)
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static File getCoveredVideo(JCas videoFileView, AudioToken audioToken){
        return getCoveredVideo(videoFileView, audioToken, "mp4");
    }

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param videoFileView The view containing the entire video file in its sofa string (If null, tries to auto-detect)
     * @param audioToken The audio token class (like AudioToken)
     * @param targetFormat File format for the output file. (like "mp4" or "webm")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static File getCoveredVideo(JCas videoFileView, AudioToken audioToken, String targetFormat){

        String inputFileName = "temp_" + videoFileView.getViewName();
        String outputFileName = getOutputName(audioToken, targetFormat);

        if(!new File("temp_" + videoFileView.getViewName()).exists()) {
            // Convert encoded string to file
            OutputStream stream = null;
            try {
                stream = new FileOutputStream(inputFileName);
                stream.write(Base64.decodeBase64(videoFileView.getSofaDataString()));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        File inputFile = new File(inputFileName);
        inputFile.deleteOnExit();

        executeFFMpeg(inputFile.getAbsolutePath(), outputFileName, audioToken.getTimeStart(), audioToken.getTimeEnd());

        File outputFile = new File(outputFileName);
        outputFile.deleteOnExit();

        return outputFile;
    }

    private static void getEveryVideoSegment(JCas videoViewCas, List<String> commands){

        String inputFileName = "temp_" + videoViewCas.getViewName();

        if(!new File("temp_" + videoViewCas.getViewName()).exists()) {
            // Convert encoded string to file
            OutputStream stream = null;
            try {
                stream = new FileOutputStream(inputFileName);
                stream.write(Base64.decodeBase64(videoViewCas.getSofaDataString()));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        File inputFile = new File(inputFileName);
        inputFile.deleteOnExit();

        executeFFMpeg(inputFile.getAbsolutePath(), commands);
    }

    /**
     * Tries to find the view which contains the entire video file in its sofa string
     * @param fromJCas JCas to search the views in
     * @throws CASException
     */
    public static JCas predictVideoView(JCas fromJCas) throws CASException {
        Iterator<JCas> iter = fromJCas.getViewIterator();

        while(iter.hasNext()){
            JCas view = iter.next();

            if(view.getSofaMimeType().startsWith("video/")){
                return view;
            }
        }

        return fromJCas;
    }

    private static void executeFFMpeg(String absoluteInputPath, String output, float startTime, float endTime){
        try {
            // -ss: seeking (skipping forward x seconds)
            // -t: duration
            ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-ss,", Float.toString(startTime), "-t", Float.toString(endTime - startTime),  "-i", absoluteInputPath, output);

            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            p.waitFor();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void executeFFMpeg(String absoluteInputPath, List<String> commands){
        try {
            // -ss: seeking (skipping forward x seconds)
            // -t: duration
            ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-i", absoluteInputPath);

            for(String outputCommand : commands){
                pb.command().addAll(Arrays.stream(outputCommand.split(" ")).toList());
            }

            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            p.waitFor();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String getOutputName(AudioToken audioToken, String format){
        if(format.startsWith(".")){
            format = format.substring(1);
        }

        return audioToken._id() + "_" + audioToken.getTimeStart() + "-" + audioToken.getTimeEnd() + "." + format;
    }

}

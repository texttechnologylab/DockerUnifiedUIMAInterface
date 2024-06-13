package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.commons.codec.binary.Base64;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.type.AudioToken;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class MultimodalUtil {

    public static <T extends AudioToken>List<File> getAllCoveredAudio(JCas audioTokenCas, Class<T> audioToken){
        return getAllCoveredAudio(audioTokenCas, audioToken, null, "wav");
    }

    public static <T extends AudioToken>List<File> getAllCoveredAudio(JCas audioTokenCas, Class<T> audioToken, String targetFormat){
        return getAllCoveredAudio(audioTokenCas, audioToken, null, targetFormat);
    }

    public static <T extends AudioToken>List<File> getAllCoveredAudio(JCas audioTokenCas, Class<T> audioToken, String audioView, String targetFormat){

        List<File> files = new ArrayList<>();
        List<String> commands = new LinkedList<>();

        JCasUtil.select(audioTokenCas, audioToken).forEach(token -> {
            commands.add(String.format("-ss %s -t %s %s",
                    token.getTimeStart(),
                    token.getTimeEnd() - token.getTimeStart(),
                    getOutputName(token, targetFormat)));


            File file = new File(getOutputName(token, targetFormat));
            file.deleteOnExit();
            files.add(file);
        });

        MultimodalUtil.getEveryAudioSegment(audioTokenCas, audioView, commands);

        return files;
    }

    public static File getCoveredAudio(JCas audioTokenCas, AudioToken audioToken){
        return getCoveredAudio(audioTokenCas, audioToken, null, "wav");
    }

    public static File getCoveredAudio(JCas audioTokenCas, AudioToken audioToken, String targetFormat){
        return getCoveredAudio(audioTokenCas, audioToken, null, targetFormat);
    }

    public static File getCoveredAudio(JCas audioTokenCas, AudioToken audioToken, String audioView, String targetFormat){

        String inputFileName = "temp_" + audioView;
        String outputFileName = getOutputName(audioToken, targetFormat);

        if(!new File(inputFileName).exists()) {
            // Convert encoded string to file
            OutputStream stream = null;
            try {
                stream = new FileOutputStream(inputFileName);
                if (audioView == null)
                    stream.write(Base64.decodeBase64(audioTokenCas.getSofaDataString()));
                else
                    stream.write(Base64.decodeBase64(audioTokenCas.getView(audioView).getSofaDataString()));
            } catch (IOException | CASException e) {
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

    private static void getEveryAudioSegment(JCas audioTokenCas, String audioView, List<String> commands){

        String inputFileName = "temp_" + audioView;

        if(!new File(inputFileName).exists()) {
            // Convert encoded string to file
            OutputStream stream = null;
            try {
                stream = new FileOutputStream(inputFileName);
                if (audioView == null)
                    stream.write(Base64.decodeBase64(audioTokenCas.getSofaDataString()));
                else
                    stream.write(Base64.decodeBase64(audioTokenCas.getView(audioView).getSofaDataString()));
            } catch (IOException | CASException e) {
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


    public static <T extends AudioToken>List<File> getAllCoveredVideo(JCas videoViewCas, JCas audioViewCas, Class<T> audioToken){
        return getAllCoveredVideo(videoViewCas, audioViewCas, audioToken, "mp4");
    }

    public static <T extends AudioToken>List<File> getAllCoveredVideo(JCas videoViewCas, JCas audioViewCas, Class<T> audioToken, String targetFormat){

        List<File> files = new ArrayList<>();
        List<String> commands = new LinkedList<>();

        JCasUtil.select(audioViewCas, audioToken).forEach(token -> {
            commands.add(String.format("-ss %s -t %s %s",
                    token.getTimeStart(),
                    token.getTimeEnd() - token.getTimeStart(),
                    getOutputName(token, targetFormat)));


            File file = new File(getOutputName(token, targetFormat));
            file.deleteOnExit();
            files.add(file);
        });

        MultimodalUtil.getEveryVideoSegment(videoViewCas, commands);

        return files;
    }


    public static File getCoveredVideo(JCas videoViewCas, AudioToken audioToken){
        return getCoveredVideo(videoViewCas, audioToken, "mp4");
    }

    public static File getCoveredVideo(JCas videoViewCas, AudioToken audioToken, String targetFormat){

        String inputFileName = "temp_" + videoViewCas.getViewName();
        String outputFileName = getOutputName(audioToken, targetFormat);

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

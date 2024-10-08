package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.apache.commons.codec.binary.Base64;
import org.apache.uima.cas.CASException;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.texttechnologylab.annotation.AnnotationComment;
import org.texttechnologylab.annotation.type.AudioToken;
import org.texttechnologylab.annotation.type.Coordinate;
import org.texttechnologylab.annotation.type.SubImage;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Path2D;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.List;

public class MultimodalUtil {

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param annotationClass TThe annotation the covered elements are derived from
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends Annotation> List<File> getAllCoveredAudio(JCas audioTokenView, Class<T> annotationClass) throws CASException {
        return getAllCoveredAudio(audioTokenView, null, annotationClass, "wav");
    }

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param annotationClass The annotation the covered elements are derived from
     * @param targetFormat File format for the output file. (like "wav" or "mp3")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends Annotation> List<File> getAllCoveredAudio(JCas audioTokenView, Class<T> annotationClass, String targetFormat) throws CASException {
        return getAllCoveredAudio(audioTokenView, null, annotationClass, targetFormat);
    }

    /**
     * Converts each AudioTokens into its own audio snippet.
     * @param audioTokenView The view in which the audio tokens are stored
     * @param annotationClass The annotation the covered elements are derived from
     * @param audioFileView The view containing the entire audio file in its sofa string (If null, tries to auto-detect)
     * @param targetFormat File format for the output file. (like "wav" or "mp3")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends Annotation> List<File> getAllCoveredAudio(JCas audioTokenView, JCas audioFileView, Class<T> annotationClass, String targetFormat) throws CASException {

        List<File> files = new ArrayList<>();
        List<String> commands = new LinkedList<>();

        JCasUtil.select(audioTokenView, annotationClass).forEach(annotation -> {

            float startTime = Integer.MAX_VALUE;
            float endTime = 0;

            List<AudioToken> tokens = JCasUtil.selectOverlapping(AudioToken.class, annotation).stream().toList();

            for(AudioToken token : tokens){
                System.out.println(token.getTimeStart() + " " + token.getTimeEnd());
                if(token.getTimeStart() < startTime)
                    startTime = token.getTimeStart();

                if(token.getTimeEnd() > endTime)
                    endTime = token.getTimeEnd();
            }

            if(startTime == Integer.MAX_VALUE) {
                return;
            }

            commands.add(String.format("-ss %s -t %s %s",
                    startTime,
                    endTime - startTime,
                    getOutputName(audioTokenView, annotation, targetFormat)));


            File file = new File(getOutputName(audioTokenView, annotation, targetFormat));
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
        return getCoveredAudio(audioTokenView, null, audioToken, "wav");
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
        return getCoveredAudio(audioTokenView, null, audioToken, targetFormat);
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
    public static File getCoveredAudio(JCas audioTokenView, JCas audioFileView, AudioToken audioToken, String targetFormat) throws CASException {

        if(audioFileView == null)
            audioFileView = findAudioView(audioTokenView);

        String inputFileName = "temp_" + audioFileView.getViewName();
        String outputFileName = getOutputName(audioTokenView, audioToken, targetFormat);

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

        inputFile.delete();

        File outputFile = new File(outputFileName);
        outputFile.deleteOnExit();

        return outputFile;
    }

    private static void getEveryAudioSegment(JCas audioTokenCas, JCas audioFileView, List<String> commands) throws CASException {

        if(audioFileView == null)
            audioFileView = findAudioView(audioTokenCas);

        String inputFileName = "temp_" + audioFileView.getViewName();

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

        executeFFMpeg(inputFile.getAbsolutePath(), commands);
        inputFile.delete();
    }

    /**
     * Tries to find the view which contains the entire audio file in its sofa string
     * @param fromJCas JCas to search the views in
     * @throws CASException
     */
    public static JCas findAudioView(JCas fromJCas) throws CASException {
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
     * @param annotationClass The annotation the covered elements are derived from
     * @return List of files, each file containing the video content.
     * @throws CASException
     */
    public static <T extends Annotation> List<File> getAllCoveredVideo(JCas audioTokenView, Class<T> annotationClass) throws CASException {
        return getAllCoveredVideo(null, audioTokenView, annotationClass);
    }

    /**
     * Converts each AudioToken into its own audio snippet.
     * @param videoFileView The view containing the entire video file in its sofa string (If null, tries to auto-detect)
     * @param audioTokenView The view in which the audio tokens are stored
     * @param annotationClass The annotation the covered elements are derived from
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends Annotation> List<File> getAllCoveredVideo(JCas audioTokenView, JCas videoFileView, Class<T> annotationClass) throws CASException {
        return getAllCoveredVideo(videoFileView, audioTokenView, annotationClass, "mp4");
    }

    /**
     * Converts each AudioToken into its own audio snippet.
     * @param videoFileView The view containing the entire video file in its sofa string (If null, tries to auto-detect)
     * @param audioTokenView The view in which the audio tokens are stored
     * @param annotationClass The annotation the covered elements are derived from
     * @param targetFormat File format for the output file. (like "mp4" or "webm")
     * @return List of files, each file containing the audio content.
     * @throws CASException
     */
    public static <T extends Annotation> List<File> getAllCoveredVideo(JCas audioTokenView, JCas videoFileView, Class<T> annotationClass, String targetFormat) throws CASException {

        if(videoFileView == null){
            videoFileView = findVideoView(audioTokenView);
        }

        List<File> files = new ArrayList<>();
        List<String> commands = new LinkedList<>();

        JCasUtil.select(audioTokenView, annotationClass).forEach(annotation -> {

            float startTime = Integer.MAX_VALUE;
            float endTime = 0;

            List<AudioToken> tokens = JCasUtil.selectOverlapping(AudioToken.class, annotation).stream().toList();

            for(AudioToken token : tokens){
                if(token.getTimeStart() < startTime)
                    startTime = token.getTimeStart();

                if(token.getTimeEnd() > endTime)
                    endTime = token.getTimeEnd();
            }

            if(startTime == Integer.MAX_VALUE)
                return;
/*
            System.out.println("============================");
            System.out.println(getOutputName(audioTokenView, annotation, targetFormat));
            System.out.println(startTime + " " + endTime);
 */

            commands.add(String.format("-ss %s -t %s %s",
                    startTime,
                    endTime - startTime,
                    getOutputName(audioTokenView, annotation, targetFormat)));


            File file = new File(getOutputName(audioTokenView, annotation, targetFormat));
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
        String outputFileName = getOutputName(videoFileView, audioToken, targetFormat);

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
    public static JCas findVideoView(JCas fromJCas) throws CASException {
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

    /**
     * Gets subimages as independent image files in .png
     * @param jCas The JCas containing SubImage Annotations
     * @return A List of files, each containing a SubImage. Files get deleted on exit
     */
    public static List<File> getSubImages(JCas jCas){
        return getSubImages(jCas, "png");
    }

    /**
     * Gets subimages as independent image files
     * @param jCas The JCas containing SubImage Annotations
     * @param extension Changes the subimages images file extension
     * @return A List of files, each containing a SubImage. Files get deleted on exit
     */
    public static List<File> getSubImages(JCas jCas, String extension) {

        if(extension.startsWith("."))
            extension = extension.substring(1);
        String finalExtension = extension;


        List<File> subImages = new ArrayList<>();

        JCasUtil.select(jCas, SubImage.class).forEach(subImage -> {
            String mainImageB64 = "";
            if(subImage.getParent().getSrc() == null){
                mainImageB64 = jCas.getSofaDataString();
            }else{
                mainImageB64 = subImage.getParent().getSrc();
            }

            byte[] base64Image = Base64.decodeBase64(mainImageB64);
            try {
                BufferedImage bImage = ImageIO.read(new ByteArrayInputStream(base64Image));

                Polygon polygon = new Polygon();

                for(int i = 0; i < subImage.getCoordinates().size(); i++){

                        Coordinate coordniate = subImage.getCoordinates().get(i);
                        polygon.addPoint(coordniate.getX(), coordniate.getY());
                }

                Rectangle bounds = polygon.getBounds();

                BufferedImage bSubImage;
                if(finalExtension.equals("png") || finalExtension.equals("tiff") || finalExtension.equals("gif") || finalExtension.equals("apng"))
                    bSubImage = new BufferedImage(bounds.width, bounds.height, BufferedImage.TYPE_INT_ARGB);  // Alpha channel support
                else
                    bSubImage = new BufferedImage(bounds.width, bounds.height, BufferedImage.TYPE_INT_RGB);  // No alpha

                // Copy pixels form main image to new subimage
                for (int x = bounds.x; x < bounds.x + bounds.width; x++) {
                    for (int y = bounds.y; y < bounds.y + bounds.height; y++) {
                        if(polygon.contains(x, y)){
                            int iColor = bImage.getRGB(x, y);
                            bSubImage.setRGB(x - bounds.x, y - bounds.y, iColor);
                        }
                    }
                }

                // Create output file
                File outputFile = new File(getOutputName(jCas, subImage, finalExtension));
                outputFile.deleteOnExit();

                RenderedImage rendImage = bSubImage;
                ImageIO.write(rendImage, finalExtension, outputFile);

                subImages.add(outputFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return subImages;
    }

    private static String getOutputName(JCas jCas, AudioToken audioToken, String format){
        if(format.startsWith(".")){
            format = format.substring(1);
        }

        String documentId = "";
        if (JCasUtil.select(jCas, DocumentMetaData.class).size() > 0) {
            DocumentMetaData meta = DocumentMetaData.get(jCas);
            documentId = meta.getDocumentId() + "_";
        }

        System.out.println("OUTPUT FILE: " + documentId + audioToken._id() + "_" + audioToken.getTimeStart() + "-" + audioToken.getTimeEnd() + "." + format);
        return documentId + audioToken._id() + "_" + audioToken.getTimeStart() + "-" + audioToken.getTimeEnd() + "." + format;
    }

    private static String getOutputName(JCas jCas, Annotation annotation, String format){
        if(format.startsWith(".")){
            format = format.substring(1);
        }

        String documentId = "";
        if (JCasUtil.select(jCas, DocumentMetaData.class).size() > 0) {
            DocumentMetaData meta = DocumentMetaData.get(jCas);
            documentId = meta.getDocumentId() + "_";
        }

        return documentId + annotation._id() + "_" + annotation.getBegin()+ "-" + annotation.getEnd() + "." + format;
    }

    private static String getOutputName(JCas jCas, SubImage subImage, String format){

        String documentId = "";
        if (JCasUtil.select(jCas, DocumentMetaData.class).size() > 0) {
            DocumentMetaData meta = DocumentMetaData.get(jCas);
            documentId = meta.getDocumentId() + "_";
        }

        return documentId + subImage.getParent()._id() + "_" + subImage._id() + "." + format;
    }

}

package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import org.apache.commons.codec.binary.Base64;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.jcas.JCas;
import org.texttechnologylab.annotation.type.AudioToken;

import java.io.*;

public class MultimodalUtil {

    public static <T extends AnnotationFS> File getCoveredAudio(JCas jcas, AudioToken audioToken){
        return getCoveredAudio(jcas, audioToken, null, "wav");
    }

    public static <T extends AnnotationFS> File getCoveredAudio(JCas jcas, AudioToken audioToken, String format){
        return getCoveredAudio(jcas, audioToken, null, format);
    }

    public static <T extends AnnotationFS> File getCoveredAudio(JCas jcas, AudioToken audioToken, String audioView, String format){

        if(format.startsWith(".")){
            format = format.substring(1);
        }

        String inputFileName = "temp" + audioToken._id() + "_" + audioToken.getTimeStart() + "-" + audioToken.getTimeEnd();
        String outputFileName = audioToken._id() + "_" + audioToken.getTimeStart() + "-" + audioToken.getTimeEnd() + "." + format;


        // Convert encoded string to file
        OutputStream stream = null;
        try {
            stream = new FileOutputStream(inputFileName);
            if(audioView == null)
                stream.write(Base64.decodeBase64(jcas.getSofaDataString()));
            else
                stream.write(Base64.decodeBase64(jcas.getView(audioView).getSofaDataString()));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException | CASException e) {
            e.printStackTrace();
        } finally {
            if(stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        try {
            // -ss: seeking (skipping forward x seconds)
            // -t: duration
            Runtime.getRuntime().exec(String.format("ffmpeg -ss %s -i %s -t %s %s",
                    audioToken.getTimeStart(),
                    inputFileName,
                    audioToken.getTimeEnd() - audioToken.getTimeStart(),
                    "C:/test/" + outputFileName
            ));
        } catch (IOException e) {
            e.printStackTrace();
        }

        audioToken.getTimeEnd();
        audioToken.getCoveredText();

        File inputFile = new File(inputFileName);
        inputFile.deleteOnExit();

        File outputFile = new File(outputFileName);
        outputFile.deleteOnExit();

        return outputFile;
    }

}

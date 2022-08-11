package org.texttechnologylab.DockerUnifiedUIMAInterface.connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DocToSentencesTest {
    StringBuffer doc;
    int begin = 0;
    int index = begin;
    int end;
    public DocToSentencesTest(String readFromInputStream) {
        doc = new StringBuffer(readFromInputStream);
        end = doc.length();

    }

    public static String readFromInputStream(InputStream inputStream)
            throws IOException {
        StringBuilder resultStringBuilder = new StringBuilder();
        try (BufferedReader br
                     = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                resultStringBuilder.append(line).append("\n");
            }
        }
        return resultStringBuilder.toString();
    }

    String[] listOfEndOfSentence = {". ", ".\n", ".\t"};

    ArrayList listOfEndOfSentenceArray = new ArrayList<String>(List.of(listOfEndOfSentence));
    String[] secondOrThirdIndexFromBehind = {" ", "\n", "\t"};
    ArrayList secondOrThirdIndexFromBehindArray = new ArrayList<String>(List.of(secondOrThirdIndexFromBehind));

    public List<String> docToList() throws StackOverflowError {
        List<String> listOfSentences = new ArrayList<>();

        while(end!=0){
            StringBuffer sentence = null;
            if(doc.length()<=3){
                // nur verübergehend
                sentence = new StringBuffer(doc+"____. ");

            }else {
                sentence = new StringBuffer(doc.substring(0,index));

            }

            if(end<=0){
                System.out.println("finished");

            }else {
                String lastTwoChar = "";
                String thirdCharFromBehind = "";
                String _23char = "";
                char lastChar = 0;
                String secondChar = "";
                if(index > (begin+3)){
                    lastTwoChar = sentence.substring(index-2, index);
                    thirdCharFromBehind = sentence.substring(index-3, index-2);
                    secondChar = sentence.substring(index-2, index-1);
                    lastChar = sentence.charAt(index-1);
                    _23char = sentence.substring(index-3, index-1);

                }
                if(listOfEndOfSentenceArray.contains(lastTwoChar)
                        &&
                        !secondOrThirdIndexFromBehindArray.contains(thirdCharFromBehind)
                ){
                    String theSentence = sentence.substring(begin, index);
                    //System.out.println(theSentence);
                    listOfSentences.add(theSentence);
                    doc.replace(0, index, "");
                    end= doc.length();

                    index = begin;


                }
                else if (!secondOrThirdIndexFromBehindArray.contains(thirdCharFromBehind)
                        && secondChar.contains(".")
                        && Character.isUpperCase(lastChar)
                ) {
                    String theSentence = sentence.substring(begin, index-1);
                    //System.out.println(theSentence);

                    listOfSentences.add(theSentence);
                    doc.replace(0, index-1, "");
                    end= doc.length();
                    index = begin;

                }else {
                    index += 1;
                }


            }

        }
        System.out.println(listOfSentences);
        return listOfSentences;

    }

    public static void main(String[] args) throws IOException {
        InputStream inputStream = DocToSentencesTest.class.getResourceAsStream("/sample_splitted/sample_103226.txt");
        DocToSentencesTest docToSentences = new DocToSentencesTest(readFromInputStream(inputStream));
        docToSentences.docToList();
    }
}

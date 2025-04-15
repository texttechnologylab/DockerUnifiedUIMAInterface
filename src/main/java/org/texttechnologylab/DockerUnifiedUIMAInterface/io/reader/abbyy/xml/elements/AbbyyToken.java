package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.StringList;
import org.texttechnologylab.annotation.ocr.abbyy.Token;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class AbbyyToken extends AbstractAnnotation {
    private static final AbbyyToken SPACE = new AbbyyToken(" ");
    private static final AbbyyToken TAB = new AbbyyToken("\t");
    private static final AbbyyToken NEWLINE = new AbbyyToken("\n");

    private ArrayList<StringBuilder> subTokenList = new ArrayList<>(List.of(new StringBuilder()));
    private ArrayList<AbbyyChar> charList;

    public AbbyyToken() {
    }

    public AbbyyToken(String text) {
        this.addChar(null, text);
    }

    @Override
    public Token into(JCas jcas, int start, int end) {
        Token token = new Token(jcas, start, end);
        if (charList != null && !charList.isEmpty()) {
            token.setSuspiciousChars(charList.stream().mapToInt(AbbyyChar::getSuspicious).sum());
            token.setIsWordFromDictionary(charList.stream().anyMatch(AbbyyChar::isWordFromDictionary));
            token.setIsWordNormal(charList.stream().anyMatch(AbbyyChar::isWordNormal));
            token.setIsWordNumeric(charList.stream().anyMatch(AbbyyChar::isWordNumeric));
            token.setMeanCharConfidence((float) charList.stream().mapToInt(AbbyyChar::getConfidence).average().orElse(-1));
            token.setMinCharConfidence((short) charList.stream().mapToInt(AbbyyChar::getConfidence).min().orElse(-1));
        }
        if (subTokenList.size() > 1) {
            token.setContainsHyphen(true);
            token.setSubTokenList(StringList.create(jcas, subTokenList.stream().map(StringBuilder::toString).toArray(String[]::new)));
        }
        return token;
    }

    public void addChar(AbbyyChar abbyyChar, String text) {
        if (abbyyChar != null) {
            if (charList == null) {
                charList = new ArrayList<>();
            }
            charList.add(abbyyChar);
        }
        subTokenList.getLast().append(text);
    }

    public void addSubToken() {
        if (subTokenList == null) {
            subTokenList = new ArrayList<>();
        }
        subTokenList.add(new StringBuilder());
    }

    public int length() {
        return subTokenList == null ? 0 : subTokenList.stream().map(StringBuilder::length).reduce(0, Integer::sum);
    }

    public static AbbyyToken space() {
        return SPACE;
    }

    public static AbbyyToken tab() {
        return TAB;
    }

    public static AbbyyToken newline() {
        return NEWLINE;
    }

    public boolean isSpace() {
        return subTokenList == null || subTokenList.stream().allMatch(s -> s.toString().isBlank());
    }

    @Override
    public String toString() {
        return subTokenList.stream().map(StringBuilder::toString).collect(Collectors.joining(""));
    }
}

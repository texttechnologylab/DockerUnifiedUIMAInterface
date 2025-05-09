package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml;

import com.google.common.collect.ImmutableList;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils.bb.RelativeBoundingBox;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.xml.elements.*;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;


public class FineReaderEventHandler extends DefaultHandler {
    // Pages
    public ArrayList<AbbyyPage> pages = new ArrayList<>();
    private AbbyyPage currPage = null;
    private String nextPageId = null;
    private String nextPageUri = null;
    private Integer nextPageIndex = null;

    // Block
    public ArrayList<AbbyyBlock> blocks = new ArrayList<>();
    private AbbyyBlock currBlock = null;

    // Paragraphs
    public ArrayList<AbbyyParagraph> paragraphs = new ArrayList<>();
    private AbbyyParagraph currParagraph = null;

    // Lines
    public ArrayList<AbbyyLine> lines = new ArrayList<>();
    private AbbyyLine currLine = null;

    // Token
    public ArrayList<AbbyyToken> tokens = new ArrayList<>();
    private AbbyyToken currToken = new AbbyyToken();

    // Switches
    public boolean lastTokenWasSpace = false;
    private boolean lastTokenWasHyphen = false;
    private boolean skipBlock = false;

    // Chars
    private AbbyyChar currChar = null;

    // Document Text
    private int textLength = 0;

    private static final Pattern spacePattern = Pattern.compile("[\\s]+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern nonWordCharacter = Pattern.compile("[^\\p{Alnum}\\-¬]+", Pattern.UNICODE_CHARACTER_CLASS);

    // Settable parameters
    protected boolean unifyWhitespaces = false;
    protected RelativeBoundingBox boundingBox = null;
    private RelativeBoundingBox.DocumentChecker boundingBoxChecker;


    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        switch (qName) {
            case "page":
                currPage = new AbbyyPage(attributes);
                currPage.setStart(textLength);
                getNextPageId().ifPresent(currPage::setPageId);
                getNextPageIndex().ifPresent(currPage::setPageIndex);
                getNextPageUri().ifPresent(currPage::setPageUri);
                if (boundingBox != null) {
                    boundingBoxChecker = boundingBox.checker(currPage.getWidth(), currPage.getHeight());
                }
                skipBlock = false;
                break;
            case "block":
                currBlock = new AbbyyBlock(attributes);
                currBlock.setStart(textLength);

                if (boundingBoxChecker != null) {
                    skipBlock = !boundingBoxChecker.contains(currBlock.getRect());
                }

                currChar = null;
                break;
            case "text":
                break;
            case "par":
                if (skipBlock)
                    break;

                currParagraph = new AbbyyParagraph(attributes);
                currParagraph.setStart(textLength);
                break;
            case "line":
                if (skipBlock)
                    break;

                currLine = new AbbyyLine(attributes);
                currLine.setStart(textLength);

                break;
            case "formatting":
                if (skipBlock)
                    break;

                if (currLine != null)
                    currLine.setFormat(new AbbyyFormat(attributes));
                break;
            case "charParams":
                if (skipBlock)
                    break;

                String wordStart = attributes.getValue("wordStart");

                if ("true".equals(wordStart) && !lastTokenWasHyphen) {
                    pushCurrToken();
                }

                currChar = new AbbyyChar(attributes);
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (qName) {
            case "page":
                addSpace();
                if (currPage != null) {
                    currPage.setEnd(textLength);
                    pages.add(currPage);
                }
                currPage = null;
                break;
            case "block":
                addSpace();
                if (currBlock != null) {
                    currBlock.setEnd(textLength);
                    blocks.add(currBlock);
                }
                currBlock = null;
                break;
            case "text":
                addSpace();
            case "par":
                addSpace();
                if (currParagraph != null) {
                    currParagraph.setEnd(textLength);
                    paragraphs.add(currParagraph);
                }
                currParagraph = null;
                break;
            case "line":
                addSpace(AbbyyToken.newline());
                if (currLine != null) {
                    currLine.setEnd(textLength);
                    lines.add(currLine);
                }
                currLine = null;
        }
        currChar = null;
    }

    private void setEnd(IIntoAnnotation annotation) {
        if (annotation != null) {
            annotation.setEnd(textLength);
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (currChar != null) {
            String text = new String(ch, start, length);

            if (currChar.isTab() || spacePattern.matcher(text).matches()) {
                addSpace();
            } else if (nonWordCharacter.matcher(text).matches()) {
                addNonWordToken(currChar, text);
            } else {
                addWordToken(text);
            }
        }
        currChar = null;
    }

    private void addSpace() {
        addSpace(AbbyyToken.space());
    }

    private void addSpace(AbbyyToken space) {
        // Do not add spaces if the preceding token is a space, the ¬ hyphenation character or there has not been any token
        if (lastTokenWasSpace || lastTokenWasHyphen || currToken == null)
            return;

        if (unifyWhitespaces) {
            space = AbbyyToken.space();
        }

        // If the current token already contains characters, create a new token for the space
        pushCurrToken();

        // Add the space character and increase token count
        tokens.add(space);
        textLength++;
        lastTokenWasSpace = true;
    }

    private void addNonWordToken(AbbyyChar abbyyChar, String text) {
        // If the current token already contains characters, create a new token for the non-word token
        pushCurrToken();

        lastTokenWasSpace = false;
        lastTokenWasHyphen = false;

        currToken.addChar(abbyyChar, text);
        textLength += text.length();

        pushCurrToken();
    }

    private void addWordToken(String text) {
        // Add a new subtoken if there has been a ¬ and it was followed by a character
        if (lastTokenWasHyphen && !lastTokenWasSpace) {
            currToken.addSubToken();
        }
        lastTokenWasSpace = false;

        // The hyphen character ¬ does not contribute to the total character count
        if (text.equals("¬")) {
            lastTokenWasHyphen = true;
        } else {
            lastTokenWasHyphen = false;
            currToken.addChar(currChar, text);
            textLength += text.length();
        }
    }

    private void pushCurrToken() {
        if (currToken.length() > 0) {
            currToken.setEnd(textLength);
            tokens.add(currToken);
        }
        currToken = new AbbyyToken();
        currToken.setStart(textLength);
    }

    public void setNextPageId(String pageId) {
        this.nextPageId = pageId;
    }

    private Optional<String> getNextPageId() {
        if (nextPageId == null) {
            return Optional.empty();
        }
        return Optional.of(nextPageId);
    }

    public void setNextPageIndex(int pageIndex) {
        this.nextPageIndex = pageIndex;
    }

    private Optional<Integer> getNextPageIndex() {
        if (nextPageIndex == null) {
            return Optional.empty();
        }
        return Optional.of(nextPageIndex);
    }

    public void setNextPageUri(String pageUri) {
        this.nextPageUri = pageUri;
    }

    private Optional<String> getNextPageUri() {
        if (nextPageUri == null) {
            return Optional.empty();
        }
        return Optional.of(nextPageUri);
    }

    public void setBoundingBox(RelativeBoundingBox boundingBox) {
        this.boundingBox = boundingBox;
    }

    public void setUnifyWhitespaces(boolean unifyWhitespaces) {
        this.unifyWhitespaces = unifyWhitespaces;
    }

    public static class ParsedDocument {
        final public ImmutableList<AbbyyPage> pages;
        final public ImmutableList<AbbyyBlock> blocks;
        final public ImmutableList<AbbyyParagraph> paragraphs;
        final public ImmutableList<AbbyyLine> lines;
        final public ImmutableList<AbbyyToken> tokens;
        final public boolean lastTokenWasSpace;

        public ParsedDocument(
                List<AbbyyPage> pages,
                List<AbbyyBlock> blocks,
                List<AbbyyParagraph> paragraphs,
                List<AbbyyLine> lines,
                List<AbbyyToken> tokens,
                boolean lastTokenWasSpace
        ) {
            this.pages = ImmutableList.copyOf(pages);
            this.blocks = ImmutableList.copyOf(blocks);
            this.paragraphs = ImmutableList.copyOf(paragraphs);
            this.lines = ImmutableList.copyOf(lines);
            this.tokens = ImmutableList.copyOf(tokens);
            this.lastTokenWasSpace = lastTokenWasSpace;
        }
    }

    public ParsedDocument getParsedDocument() {
        for (String elementName : new String[]{"page", "block", "text", "par", "line"}) {
            try {
                endElement(null, null, elementName);
            } catch (SAXException e) {
                throw new RuntimeException(e);
            }
        }

        pushCurrToken();
        return new ParsedDocument(pages, blocks, paragraphs, lines, tokens, lastTokenWasSpace);
    }
}

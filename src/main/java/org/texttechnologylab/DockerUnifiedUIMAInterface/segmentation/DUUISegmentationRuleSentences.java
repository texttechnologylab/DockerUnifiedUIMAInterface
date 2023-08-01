package org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation;

import org.apache.uima.jcas.JCas;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This segmentation rules tries to improve segmentation on sentence borders by forbidding to split "brackets" and similar.
 */
public class DUUISegmentationRuleSentences implements IDUUISegmentationRule {
    protected static Set<Character> openingBrackets = Set.of(
            '(', '[', '{', '<'
    );

    protected static Set<Character> closingBrackets = Set.of(
            ')', ']', '}', '>'
    );

    protected static Set<Character> simpleCharacters = Set.of(
            '\n', '\r', '\t'
    );

    // Only consider the previous/following N characters
    public static final int WINDOW_SIZE_DEFAULT = 10;
    protected int windowSize = WINDOW_SIZE_DEFAULT;

    public DUUISegmentationRuleSentences withWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    @Override
    public boolean canSegment(boolean resultRuleBefore, int begin, int end, JCas jCas, IDUUISegmentationStrategy segmentationStrategy) {
        String docText = jCas.getDocumentText();

        // Simple check:
        // check left window for opening brackets
        // check right window for opening brackets
        // in either case we do not want to split as we are inside a bracket
        // Note that we have to check for closing brackets on the left as well to prevent "skipping" these, and vice versa on the right
        // Also check simple characters like line breaks, tabs, etc.

        Map<Character, Integer> leftBracketCount = new HashMap<>();
        int leftStart = Math.max(0, end-1);
        int leftEnd = Math.max(0, end-windowSize);
        for (int i = leftStart; i > leftEnd; i--) {
            try {
                Character c = docText.charAt(i);

                if (simpleCharacters.contains(c)) {
                    return false;
                }

                if (closingBrackets.contains(c)) {
                    leftBracketCount.put(c, leftBracketCount.getOrDefault(c, 0) - 1);
                }
                if (openingBrackets.contains(c)) {
                    // found opening bracket, check if in this window there was a closing bracket already
                    // if not, we cant split here
                    if (leftBracketCount.getOrDefault(c, 0) >= 0) {
                        return false;
                    }
                    // else adjust bracket count
                    leftBracketCount.put(c, leftBracketCount.getOrDefault(c, 0) + 1);
                    // we dont break here as other brackets might be in the window as well
                }
            }
            catch (Exception e) {
                //
            }
        }

        Map<Character, Integer> rightBracketCount = new HashMap<>();
        int rightStart = Math.min(docText.length()-1, end);
        int rightEnd = Math.min(docText.length()-1, end+windowSize);
        for (int i = rightStart; i <= rightEnd; i++) {
            try {
                Character c = docText.charAt(i);

                if (simpleCharacters.contains(c)) {
                    return false;
                }

                if (openingBrackets.contains(c)) {
                    rightBracketCount.put(c, rightBracketCount.getOrDefault(c, 0) + 1);
                }
                if (closingBrackets.contains(c)) {
                    // found closing bracket, check if in this window there was a opening bracket already
                    // if not, we cant split here
                    if (rightBracketCount.getOrDefault(c, 0) <= 0) {
                        return false;
                    }
                    // else adjust bracket count
                    rightBracketCount.put(c, rightBracketCount.getOrDefault(c, 0) - 1);
                    // we dont break here as other brackets might be in the window as well
                }
            }
            catch (Exception e) {
                //
            }
        }

        // By default, we respect the last rule
        return resultRuleBefore;
    }
}

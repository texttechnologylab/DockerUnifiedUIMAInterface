package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils;

import org.apache.uima.resource.ResourceInitializationException;

import java.util.ArrayList;
import java.util.List;

public class MatchRules {
    public static final String INCLUDE_PREFIX = "[+]";
    public static final String EXCLUDE_PREFIX = "[-]";

        public final List<String> includes = new ArrayList<>();
        public final List<String> excludes = new ArrayList<>();

        public MatchRules(String[] patterns, String defaultInclude) throws ResourceInitializationException {
            if (patterns != null) {
                for (String pattern : patterns) {
                    if (pattern.startsWith(INCLUDE_PREFIX)) {
                        includes.add(pattern.substring(INCLUDE_PREFIX.length()));
                    } else if (pattern.startsWith(EXCLUDE_PREFIX)) {
                        excludes.add(pattern.substring(EXCLUDE_PREFIX.length()));
                    } else if (pattern.matches("^\\[.\\].*")) {
                        throw new ResourceInitializationException(new IllegalArgumentException(
                                "Patterns have to start with " + INCLUDE_PREFIX + " or "
                                        + EXCLUDE_PREFIX + "."));
                    } else {
                        includes.add(pattern);
                    }
                }
            }
            if (includes.isEmpty()) {
                includes.add(defaultInclude);
            }
        }

        public boolean hasExcludes() {
            return !excludes.isEmpty();
        }
    }
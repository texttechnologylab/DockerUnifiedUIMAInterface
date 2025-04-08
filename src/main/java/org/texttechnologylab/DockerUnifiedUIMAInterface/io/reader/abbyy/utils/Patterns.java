package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils;

import java.util.regex.Pattern;

public class Patterns {
        public static final Pattern weirdNumberTable = Pattern.compile("^[\\t \\d\\pP\\pS]+$", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern weirdLetterTable = Pattern.compile("^(\\S{1,2} )+$", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern yearPattern = Pattern.compile("^[ \\t]*.?\\pN{4}.?[ \\t]*$", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern letterPattern = Pattern.compile("[\\p{Alpha} ,.\\-]", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern otherPattern = Pattern.compile("[^\\p{Alpha} ,.\\-]", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern alnumPattern = Pattern.compile("[\\p{Alnum} ,.\\-]", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern nonAlnumPattern = Pattern.compile("[^\\p{Alnum} ,.\\-]", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern wordPattern = Pattern.compile("(?:[\\p{Z}\\-_]|^)(?:[\\p{L}]+|[\\p{Nd}]+)|(?:[\\p{L}]+|[\\p{Nd}]+)(?:[\\p{Zs}\\-_]|[\\n\\r\\f]$)", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern spacePattern = Pattern.compile("[ \\t]", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern tokenPattern = Pattern.compile("(?:[\\p{Z}]|^)[\\p{L}\\p{P}\\p{Sm}\\p{N}\\p{Sc}♂♀¬°½±^]+|[\\p{L}\\p{P}\\p{Sm}\\p{N}\\p{Sc}♂♀¬°½±^]+(?:[\\p{Zs}]|[\\n\\r\\f]$)",
                Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern allNonSpacePattern = Pattern.compile("[^\\p{Z}]", Pattern.UNICODE_CHARACTER_CLASS);
        public static final Pattern nonGarbageLine = Pattern.compile("^[\\w\\p{Z}♂♀¬°½±]{3,}$|^[\\w\\p{Z}\\p{P}\\p{Sm}\\p{N}\\p{Sc}♂♀¬°½±^]{5,}$|^[\\p{Z}]*$|^[\\p{N}\\p{Punct}\\p{Z}]+$", Pattern.UNICODE_CHARACTER_CLASS);

    }
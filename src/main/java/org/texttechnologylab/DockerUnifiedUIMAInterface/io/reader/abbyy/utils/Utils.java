package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy.utils;

import com.google.common.base.Strings;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

public class Utils {
    public static int parseInt(String s) {
        return Strings.isNullOrEmpty(s) ? 0 : Integer.parseInt(s);
    }

    public static int parseIntOr(String s, int defaultValue) {
        return Strings.isNullOrEmpty(s) ? defaultValue : Integer.parseInt(s);
    }

    public static float parseFloat(String s) {
        return Strings.isNullOrEmpty(s) ? 0f : Float.parseFloat(s);
    }

    public static boolean parseBoolean(String s) {
        return !Strings.isNullOrEmpty(s) && (
                StringUtils.isNumeric(s) && Integer.parseInt(s) > 0
                ||
                Boolean.parseBoolean(s)
        );
    }

    public static String ensureSuffix(String string, String suffix) {
        if (string.endsWith(suffix)) {
            return string;
        } else {
            return string + suffix;
        }
    }

//    public static HashSet<String> loadDict(String pDictPath) throws IOException {
//        HashSet<String> dict = new HashSet<>();
//        if (pDictPath != null) {
//            try (BufferedReader br = new BufferedReader(new FileReader(new File(pDictPath)))) {
//                dict = br.lines().map(String::trim).collect(Collectors.toCollection(HashSet::new));
//            }
//        }
//        return dict;
//    }

//    public static Optional<String> getCommonPathPrefix(ArrayList<String> inputFiles) {
//        String first = inputFiles.getFirst();
//        Path commonPath = Paths.get(first);
//        if (!commonPath.toFile().isDirectory()) {
//            commonPath = commonPath.getParent();
//        }
//        while (commonPath != null) {
//            final String currentPath = commonPath.toString();
//            if (inputFiles.stream().allMatch(p -> p.startsWith(currentPath))) {
//                return Optional.of(currentPath);
//            }
//            commonPath = commonPath.getParent();
//        }
//
//        return Optional.empty();
//    }

//    public static ArrayList<File> getFilesInFileTree(File parent) {
//        final ArrayList<File> files = new ArrayList<>();
//        for (File file : Files.fileTreeTraverser().preOrderTraversal(parent)) {
//            if (file.isFile()) {
//                files.add(file);
//            }
//        }
//        return files;
//    }
}

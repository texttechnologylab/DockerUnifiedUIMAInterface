package org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.abbyy;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LanguageToLocaleLookup {
    static final Set<Locale> inLocales = new HashSet<>(Set.of(Locale.getDefault(Locale.Category.DISPLAY)));
    static final Map<String, Locale> localeLookup = Locale.availableLocales()
            .collect(Collectors.toMap(
                    Locale::getDisplayLanguage,
                    l -> l,
                    (a, b) -> Locale.forLanguageTag(a.getLanguage())
            ));

    public static void addInLocale(Locale inLocale) {
        if (!inLocales.contains(inLocale)) {
            localeLookup.putAll(
                    Locale.availableLocales()
                            .collect(Collectors.toMap(
                                    Locale::getDisplayLanguage,
                                    l -> l,
                                    (a, b) -> Locale.forLanguageTag(a.getLanguage())
                            ))
            );
            inLocales.add(inLocale);
        }
    }

    public static Locale get(String language) {
        return localeLookup.get(language);
    }

    public static Locale getOrDefault(String language, Locale defaultLocale) {
        return localeLookup.getOrDefault(language, defaultLocale);
    }
}

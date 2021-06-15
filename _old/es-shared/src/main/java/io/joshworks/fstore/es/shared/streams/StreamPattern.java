package io.joshworks.fstore.es.shared.streams;

import io.joshworks.fstore.es.shared.utils.StringUtils;

import java.util.Set;

public class StreamPattern {

    private static final String WILDCARD = "*";

    public static boolean matchesPattern(String streamName, Set<String> patterns) {
        if (patterns == null) {
            return false;
        }
        for (String pattern : patterns) {
            if (matches(streamName, pattern)) {
                return true;
            }
        }
        return false;
    }

    public static boolean matches(String streamName, String pattern) {
        if (StringUtils.isBlank(pattern) || StringUtils.isBlank(streamName)) {
            return false;
        }
        pattern = pattern.trim();
        boolean startWildcard = pattern.startsWith(WILDCARD);
        boolean endWildcard = pattern.endsWith(WILDCARD);
        if (startWildcard && endWildcard) {
            String patternValue = pattern.substring(1, pattern.length() - 1);
            return !patternValue.isEmpty() && streamName.contains(patternValue);
        }
        if (startWildcard) {
            String patternValue = pattern.substring(1);
            return !patternValue.isEmpty() && streamName.endsWith(patternValue);
        }
        if (endWildcard) {
            String patternValue = pattern.substring(0, pattern.length() - 1);
            return !patternValue.isEmpty() && streamName.startsWith(patternValue);
        }
        return streamName.equals(pattern);
    }

    public static boolean isWildcard(String pattern) {
        return !StringUtils.isBlank(pattern) && pattern.contains(WILDCARD);
    }

}

package io.joshworks.fstore.eventry.streams;

import java.util.Set;

import static io.joshworks.fstore.eventry.utils.StringUtils.isBlank;

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
        if (isBlank(pattern) || isBlank(streamName)) {
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
        return !isBlank(pattern) && pattern.contains(WILDCARD);
    }

}

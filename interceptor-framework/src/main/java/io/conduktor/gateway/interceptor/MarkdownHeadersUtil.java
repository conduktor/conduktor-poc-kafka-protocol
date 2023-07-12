package io.conduktor.gateway.interceptor;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MarkdownHeadersUtil {
    private static final Pattern pattern = Pattern.compile("\\s*([^\\s]+):\\s*(.*+)", Pattern.MULTILINE);
    public static final String HEADER_SEPARATOR = "---";

    public Map<String, String> extractHeaders(String markdown) {
        String headers = markdownHeaders(markdown);
        Matcher matcher = pattern.matcher(headers);
        Map<String, String> ret = new HashMap<>();
        while (matcher.find()) {
            ret.put(matcher.group(1), matcher.group(2));
        }
        return ret;
    }

    @NotNull
    private String markdownHeaders(String v) {
        int first = v.indexOf(HEADER_SEPARATOR);
        if (first == -1) {
            return "";
        }
        String newStr = v.substring(first + HEADER_SEPARATOR.length());
        int second = newStr.indexOf(HEADER_SEPARATOR);
        if (second == -1) {
            return "";
        }

        return v.substring(first + HEADER_SEPARATOR.length(), second + first + HEADER_SEPARATOR.length());
    }
}

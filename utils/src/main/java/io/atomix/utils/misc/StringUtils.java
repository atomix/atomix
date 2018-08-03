package io.atomix.utils.misc;

import java.util.ArrayList;
import java.util.List;

/**
 * Collection of various helper methods to manipulate strings.
 */
public final class StringUtils {

    private StringUtils() {
    }

    /**
     * Splits the input string with the given regex and filters empty strings.
     *
     * @param input the string to split.
     * @return the array of strings computed by splitting this string
     */
    public static String[] split(String input, String regex) {
        if (input == null) {
            return null;
        }
        String[] arr = input.split(regex);
        List<String> results = new ArrayList<>(arr.length);
        for (String a : arr) {
            if (!a.trim().isEmpty()) {
                results.add(a);
            }
        }
        return results.toArray(new String[0]);
    }
}

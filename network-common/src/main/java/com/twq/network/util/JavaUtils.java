package com.twq.network.util;

import com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaUtils {

    private static final ImmutableMap<String, TimeUnit> timeSuffixes =
            ImmutableMap.<String, TimeUnit>builder()
                    .put("us", TimeUnit.MICROSECONDS)
                    .put("ms", TimeUnit.MILLISECONDS)
                    .put("s", TimeUnit.SECONDS)
                    .put("m", TimeUnit.MINUTES)
                    .put("min", TimeUnit.MINUTES)
                    .put("h", TimeUnit.HOURS)
                    .put("d", TimeUnit.DAYS)
                    .build();

    /**
     * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
     * no suffix is provided, the passed number is assumed to be in seconds.
     */
    public static long timeStringAsSec(String str) {
        return timeStringAs(str, TimeUnit.SECONDS);
    }

    /**
     * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit.
     * The unit is also considered the default if the given string does not specify a unit.
     */
    public static long timeStringAs(String str, TimeUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !timeSuffixes.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }

            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }
}

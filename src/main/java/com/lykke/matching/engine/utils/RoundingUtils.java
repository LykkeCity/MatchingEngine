package com.lykke.matching.engine.utils;

import java.math.BigDecimal;
import java.text.DecimalFormat;

import org.jetbrains.annotations.NotNull;

import static java.math.BigDecimal.*;

public class RoundingUtils {
    private static final DecimalFormat FORMAT = initFormat(8);
    private static final DecimalFormat FORMAT2 = initFormat(2);

    private static DecimalFormat initFormat(int accuracy) {
        DecimalFormat df = new DecimalFormat("#.#");
        df.setMaximumFractionDigits(accuracy);
        return df;
    }

    @NotNull
    public static Double round(Double value, int accuracy, boolean roundUp) {
        return new BigDecimal(value).setScale(accuracy + 4, ROUND_HALF_UP).setScale(accuracy, roundUp ? ROUND_UP : ROUND_DOWN).doubleValue();
    }

    /**
     * Returns round up result if base result is zero
     */
    public static double roundNoZero(Double value, int accuracy, boolean roundUp) {
        double result = new BigDecimal(value).setScale(accuracy + 4, ROUND_HALF_UP).setScale(accuracy, roundUp ? ROUND_UP : ROUND_DOWN).doubleValue();
        if (result == 0.0) {
            result = new BigDecimal(value).setScale(accuracy + 4, ROUND_HALF_UP).setScale(accuracy, ROUND_UP).doubleValue();
        }
        return result;
    }

    public static String roundForPrint(Double value) {
        return FORMAT.format(value);
    }

    public static String roundForPrint2(Double value) {
        if (value.isNaN()) {
            return "0";
        }
        return FORMAT2.format(value);
    }

    public static BigDecimal parseDouble(Double value, int accuracy) {
        return new BigDecimal(value).setScale(accuracy + 8, ROUND_HALF_UP).setScale(accuracy, ROUND_HALF_UP);
    }
}

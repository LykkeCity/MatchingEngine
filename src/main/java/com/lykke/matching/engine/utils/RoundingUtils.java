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
        return new BigDecimal(value.toString()).setScale(Math.min(16, accuracy + 10), ROUND_HALF_UP)
                .setScale(accuracy, roundUp ? ROUND_UP : ROUND_DOWN).doubleValue();
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

    public static BigDecimal parseDouble(BigDecimal value, int accuracy) {
        return value.setScale(accuracy + 8, ROUND_HALF_UP).setScale(accuracy, ROUND_HALF_UP);
    }
}

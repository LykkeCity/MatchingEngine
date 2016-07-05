package com.lykke.matching.engine.utils;

import java.math.BigDecimal;

import static java.math.BigDecimal.*;

public class RoundingUtils {
    public static Double round(Double value, int accuracy, boolean roundUp) {
        return new BigDecimal(value).setScale(accuracy + 1, ROUND_HALF_UP).setScale(accuracy, roundUp ? ROUND_UP : ROUND_DOWN).doubleValue();
    }
}

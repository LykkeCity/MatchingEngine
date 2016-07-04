package com.lykke.matching.engine.utils;

import java.math.BigDecimal;

import static java.math.BigDecimal.ROUND_HALF_UP;

public class RoundingUtils {
    public static Double round(Double value, int accuracy) {
        return new BigDecimal(value).setScale(accuracy, ROUND_HALF_UP).doubleValue();
    }
}

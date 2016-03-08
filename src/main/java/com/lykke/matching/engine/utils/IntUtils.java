package com.lykke.matching.engine.utils;

public class IntUtils {
    public static int little2big(byte[ ] b) {
        return ((b[3]&0xff)<<24)+((b[2]&0xff)<<16)+((b[1]&0xff)<<8)+(b[0]&0xff);
    }
}

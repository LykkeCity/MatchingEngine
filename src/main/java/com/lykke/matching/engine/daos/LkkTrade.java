package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class LkkTrade extends TableServiceEntity {
    public static String PARTITION_KEY = "LkkTrade";

    public static SimpleDateFormat DATE_FORMAT = initTimeFormatter();
    private static long counter = 0;

    String assetPair;
    Double price;
    Double volume;
    Date date;

    public LkkTrade(Date date, String assetPair, Double price, Double volume) {
        super(PARTITION_KEY, generateId(date));
        this.assetPair = assetPair;
        this.price = price;
        this.volume = volume;
        this.date = date;
    }

    public static String generateId(Date date) {
        counter = ++counter % 99999;
        return String.format("%s_%05d", DATE_FORMAT.format(date), counter);
    }

    private static SimpleDateFormat initTimeFormatter() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        return format;
    }

    public String getAssetPair() {
        return assetPair;
    }

    public void setAssetPair(String assetPair) {
        this.assetPair = assetPair;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
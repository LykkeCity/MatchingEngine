package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Trade extends TableServiceEntity {
    //partition key: Client id
    //row key: generated uid

    String assetId;
    Date dateTime = new Date();
    String limitOrderId;
    String marketOrderId;
    Double volume;
    Double price;

    public static String DATE_TIME = "dt";
    public static SimpleDateFormat DATE_FORMAT = initTimeFormatter();
    private static long counter = 0;

    public Trade() {
    }

    public Trade(String partitionKey, String rowKey, String assetId, Date dateTime, String limitOrderId, String marketOrderId, Double volume, Double price) {
        super(partitionKey, rowKey);
        this.assetId = assetId;
        this.dateTime = dateTime;
        this.limitOrderId = limitOrderId;
        this.marketOrderId = marketOrderId;
        this.volume = volume;
        this.price = price;
    }

    public String getClientId() {
        return partitionKey;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getLimitOrderId() {
        return limitOrderId;
    }

    public void setLimitOrderId(String limitOrderId) {
        this.limitOrderId = limitOrderId;
    }

    public String getMarketOrderId() {
        return marketOrderId;
    }

    public void setMarketOrderId(String marketOrderId) {
        this.marketOrderId = marketOrderId;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Trade(" +
                "clientId='" + partitionKey + '\'' +
                ", uid='" + rowKey + '\'' +
                ", assetId='" + assetId + '\'' +
                ", dateTime=" + dateTime +
                ", limitOrderId='" + limitOrderId + '\'' +
                ", marketOrderId='" + marketOrderId + '\'' +
                ", volume=" + volume +
                ", price=" + price +
                ')';
    }

    public Trade cloneWithGeneratedId() {
        return new Trade(DATE_TIME, rowKey, assetId, dateTime, limitOrderId, marketOrderId, volume, price);
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
}
package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

import java.util.Date;

public class BestPrice extends TableServiceEntity {
    //partition key = "Feed"
    //rowkey = asset

    public static String FEED = "Feed";

    Double ask = 0.0;
    Double bid = 0.0;
    Date dateTime = new Date();

    public BestPrice() {
    }

    public BestPrice(String asset, Double ask, Double bid) {
        super(FEED, asset);
        this.ask = ask;
        this.bid = bid;
    }

    public Double getAsk() {
        return ask;
    }

    public void setAsk(Double ask) {
        this.ask = ask;
    }

    public Double getBid() {
        return bid;
    }

    public void setBid(Double bid) {
        this.bid = bid;
    }

    public Date getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        this.dateTime = dateTime;
    }

    public String getAssetId() {
        return rowKey;
    }
}
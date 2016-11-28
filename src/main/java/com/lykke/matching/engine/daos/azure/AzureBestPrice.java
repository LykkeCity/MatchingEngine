package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.Date;

public class AzureBestPrice extends TableServiceEntity {
    //partition key = "Feed"
    //rowkey = asset

    private static final String FEED = "Feed";

    private Double ask = 0.0;
    private Double bid = 0.0;
    private Date dateTime = new Date();

    public AzureBestPrice() {
    }

    public AzureBestPrice(String asset, Double ask, Double bid) {
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
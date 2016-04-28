package com.lykke.matching.engine.daos;

import java.util.Date;

public class TradeInfo{
    String assetPair;
    Boolean isBuy;
    Double price;
    Date date;

    public TradeInfo(String assetPair, Boolean isBuy, Double price, Date date) {
        this.assetPair = assetPair;
        this.isBuy = isBuy;
        this.price = price;
        this.date = date;
    }

    public String getAssetPair() {
        return assetPair;
    }

    public void setAssetPair(String assetPair) {
        this.assetPair = assetPair;
    }

    public Boolean isBuy() {
        return isBuy;
    }

    public void setBuy(Boolean buy) {
        isBuy = buy;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
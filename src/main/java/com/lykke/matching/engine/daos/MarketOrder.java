package com.lykke.matching.engine.daos;

import java.util.Date;

public class MarketOrder extends Order {
    //partition key: client_id
    //row key: uid

    //date of execution
    Date matchedAt;
    boolean straight;

    Double dustSize;

    public MarketOrder() {
    }

    public MarketOrder(String uid, String assetPairId, String clientId, Double volume, Double price, String status,
                       Date createdAt, Date registered, String transactionId, Date matchedAt, boolean straight) {
        this(uid, assetPairId, clientId, volume, price, status, createdAt, registered, transactionId, matchedAt, straight, 0.0d);
    }

    public MarketOrder(String uid, String assetPairId, String clientId, Double volume, Double price, String status,
                       Date createdAt, Date registered, String transactionId, Date matchedAt, boolean straight, Double dustSize) {
        super(uid, assetPairId, clientId, volume, price, status, createdAt, registered, transactionId);
        this.matchedAt = matchedAt;
        this.straight = straight;
        this.dustSize = dustSize;
    }

    public boolean isBuySide() {
        return straight ? super.isBuySide() : !super.isBuySide();
    }

    public boolean isOrigBuySide() {
        return super.isBuySide();
    }

    @Override
    public String toString() {
        return "MarketOrder{"  +
                "assetPairId='" + assetPairId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", volume=" + volume +
                ", dustSize=" + dustSize +
                ", price=" + price +
                ", straight=" + straight +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", registered=" + registered +
                ", transactionId='" + transactionId + '\'' +
                ", matchedAt=" + matchedAt +
                '}';
    }

    public Date getMatchedAt() {
        return matchedAt;
    }

    public void setMatchedAt(Date matchedAt) {
        this.matchedAt = matchedAt;
    }

    public boolean getStraight() {
        return straight;
    }

    public void setStraight(boolean straight) {
        this.straight = straight;
    }

    public Double getDustSize() {
        return dustSize;
    }

    public void setDustSize(Double dustSize) {
        this.dustSize = dustSize;
    }
}
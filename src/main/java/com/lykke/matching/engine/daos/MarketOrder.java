package com.lykke.matching.engine.daos;

import java.util.Date;

public class MarketOrder extends Order {
    //partition key: client_id
    //row key: uid

    //date of execution
    Date matchedAt;
    boolean straight;

    public MarketOrder() {
    }

    public MarketOrder(String uid, String assetPairId, String clientId, Double volume, Double price, String status,
                       Date createdAt, Date registered, String transactionId, Date matchedAt, boolean straight) {
        super(uid, assetPairId, clientId, volume, price, status, createdAt, registered, transactionId);
        this.matchedAt = matchedAt;
        this.straight = straight;
    }

    public boolean isBuySide() {
        return straight ? super.isBuySide() : !super.isBuySide();
    }

    @Override
    public String toString() {
        return "MarketOrder{"  +
                "assetPairId='" + assetPairId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", volume=" + volume +
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
}
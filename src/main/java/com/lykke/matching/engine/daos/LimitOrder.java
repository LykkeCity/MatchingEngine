package com.lykke.matching.engine.daos;

import java.util.Date;

public class LimitOrder extends Order {
    //partition key: client_id
    //row key: uid
    Double remainingVolume;
    //date of execution
    Date lastMatchTime;

    public LimitOrder() {
    }

    public LimitOrder(String uid, String assetPairId, String clientId, Double volume, Double price,
                      String status, Date createdAt, Date registered, String transactionId, Double remainingVolume, Date lastMatchTime) {
        super(uid, assetPairId, clientId, volume, price, status, createdAt, registered, transactionId);
        this.remainingVolume = remainingVolume;
        this.lastMatchTime = lastMatchTime;
    }

    public Double getAbsRemainingVolume() {
        return Math.abs(remainingVolume);
    }

    public Double getRemainingVolume() {
        return remainingVolume;
    }

    public void setRemainingVolume(Double remainingVolume) {
        this.remainingVolume = remainingVolume;
    }

    public Date getLastMatchTime() {
        return lastMatchTime;
    }

    public void setLastMatchTime(Date lastMatchTime) {
        this.lastMatchTime = lastMatchTime;
    }

    @Override
    public String toString() {
        return "LimitOrder{"  +
                "assetPairId='" + assetPairId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", volume=" + volume +
                ", price=" + price +
                ", remainingVolume=" + remainingVolume +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", registered=" + registered +
                ", transactionId='" + transactionId + '\'' +
                ", lastMatchTime=" + lastMatchTime +
                '}';
    }
}
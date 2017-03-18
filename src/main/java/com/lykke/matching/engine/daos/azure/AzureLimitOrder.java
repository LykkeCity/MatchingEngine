package com.lykke.matching.engine.daos.azure;

import com.lykke.matching.engine.daos.LimitOrder;
import java.util.Date;

public class AzureLimitOrder extends AzureOrder {
    //partition key: client_id
    //row key: externalId
    private Double remainingVolume;
    //date of execution
    private Date lastMatchTime;

    public AzureLimitOrder() {
    }

    public AzureLimitOrder(String partitionKey, String uid, String assetPairId, String clientId, Double volume, Double price,
                           String status, Date createdAt, Date registered, Double remainingVolume, Date lastMatchTime) {
        super(partitionKey, uid, uid, assetPairId, clientId, volume, price, status, createdAt, registered);
        this.remainingVolume = remainingVolume;
        this.lastMatchTime = lastMatchTime;
    }

    public AzureLimitOrder(String partitionKey, LimitOrder order) {
        super(partitionKey, order.getId(), order.getId(), order.getAssetPairId(), order.getClientId(), order.getVolume(), order.getPrice(), order.getStatus(), order.getCreatedAt(), order.getRegistered());
        this.remainingVolume = order.getRemainingVolume();
        this.lastMatchTime = order.getLastMatchTime();
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

    public LimitOrder toLimitOrder() {
        return new LimitOrder(getId(), getId(), assetPairId, clientId, volume, price, status, createdAt, registered, remainingVolume, lastMatchTime);
    }

    @Override
    public String toString() {
        return "LimitOrder{"  +
                "id='" + id + '\'' +
                ", assetPairId='" + assetPairId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", volume=" + volume +
                ", price=" + price +
                ", remainingVolume=" + remainingVolume +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", registered=" + registered +
                ", lastMatchTime=" + lastMatchTime +
                '}';
    }
}
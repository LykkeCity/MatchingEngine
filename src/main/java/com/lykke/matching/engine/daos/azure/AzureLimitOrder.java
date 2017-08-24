package com.lykke.matching.engine.daos.azure;

import com.lykke.matching.engine.daos.NewLimitOrder;
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
                           String status, Date createdAt, Date registered, Double remainingVolume, Date lastMatchTime, Double reservedLimitVolume) {
        super(partitionKey, uid, uid, assetPairId, clientId, volume, price, status, createdAt, registered, reservedLimitVolume);
        this.remainingVolume = remainingVolume;
        this.lastMatchTime = lastMatchTime;
    }

    public AzureLimitOrder(String partitionKey, NewLimitOrder order) {
        super(partitionKey, order.getId(), order.getId(), order.getAssetPairId(), order.getClientId(), order.getVolume(), order.getPrice(), order.getStatus(), order.getCreatedAt(), order.getRegistered(), order.getReservedLimitVolume());
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

    public NewLimitOrder toLimitOrder() {
        return new NewLimitOrder(getId(), getId(), assetPairId, clientId, volume, price, status, createdAt, registered, remainingVolume, lastMatchTime, reservedLimitVolume);
    }

    @Override
    public String toString() {
        return "com.lykke.matching.engine.daos.LimitOrder{"  +
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
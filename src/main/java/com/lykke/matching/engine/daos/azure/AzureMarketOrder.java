package com.lykke.matching.engine.daos.azure;

import com.lykke.matching.engine.daos.MarketOrder;
import java.util.Date;

public class AzureMarketOrder extends AzureOrder {
    //partition key: client_id
    //row key: externalId

    //date of execution
    private Date matchedAt;
    private boolean straight;

    public AzureMarketOrder() {
    }

    public AzureMarketOrder(String uid, String assetPairId, String clientId, Double volume, Double price, String status,
                            Date createdAt, Date registered, Date matchedAt, boolean straight, Double reservedLimitVolume) {
        super(ORDER_ID, uid, uid, assetPairId, clientId, volume, price, status, createdAt, registered, reservedLimitVolume);
        this.matchedAt = matchedAt;
        this.straight = straight;
    }

    public AzureMarketOrder(MarketOrder order) {
        super(ORDER_ID, order.getId(), order.getId(), order.getAssetPairId(), order.getClientId(), order.getVolume(), order.getPrice(), order.getStatus(), order.getCreatedAt(), order.getRegistered(), order.getReservedLimitVolume());
        this.matchedAt = order.getMatchedAt();
        this.straight = order.getStraight();
    }

    public boolean isBuySide() {
        return straight == super.isBuySide();
    }

    public boolean isOrigBuySide() {
        return super.isBuySide();
    }

    @Override
    public String toString() {
        return "MarketOrder{"  +
                "id='" + id + '\'' +
                ", assetPairId='" + assetPairId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", volume=" + volume +
                ", price=" + price +
                ", straight=" + straight +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", registered=" + registered +
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
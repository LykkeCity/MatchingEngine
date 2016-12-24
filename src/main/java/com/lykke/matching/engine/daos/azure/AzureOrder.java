package com.lykke.matching.engine.daos.azure;

import com.google.gson.Gson;
import com.lykke.matching.engine.order.OrderSide;
import com.microsoft.azure.storage.table.TableServiceEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

abstract class AzureOrder extends TableServiceEntity {
    static final String ORDER_ID = "OrderId";
    //partition key: client_id
    //row key: uid

    String assetPairId;
    String clientId;
    // volume > 0 - Buy side, otherwise - Sell side
    Double volume;
    Double price;
    String status;
    //date from incoming message
    Date createdAt;
    //date of registering by matching engine
    Date registered;
    String transactionId;

    AzureOrder() {
    }

    AzureOrder(String partitionKey, String rowKey, String assetPairId, String clientId, Double volume, Double price, String status, Date createdAt, Date registered) {
        super(partitionKey, rowKey);
        this.assetPairId = assetPairId;
        this.clientId = clientId;
        this.volume = volume;
        this.price = price;
        this.status = status;
        this.createdAt = createdAt;
        this.registered = registered;
    }

    public String getId() {
        return rowKey;
    }

    public Double getAbsVolume() {
        return Math.abs(volume);
    }

    boolean isBuySide() {
        return volume > 0;
    }

    public OrderSide getSide() {
        return isBuySide() ? OrderSide.Buy : OrderSide.Sell;
    }

    public void addTransactionIds(List<String> ids) {
        List<String> transactions = loadTransactionsId();
        if (ids != null) {
            transactions.addAll(ids);
        }
        saveTransactionsId(transactions);
    }

    List<String> loadTransactionsId() {
        List<String> result = new ArrayList<>();
        if (transactionId != null) {
            result.addAll(Arrays.asList((String[]) (new Gson().fromJson(transactionId, String[].class))));
        }

        return result;
    }

    private void saveTransactionsId(List<String> orders) {
        this.transactionId = new Gson().toJson(orders);
    }

    public String getAssetPairId() {
        return assetPairId;
    }

    public void setAssetPairId(String assetPairId) {
        this.assetPairId = assetPairId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getRegistered() {
        return registered;
    }

    public void setRegistered(Date registered) {
        this.registered = registered;
    }

    @Override
    public String toString() {
        return "Order{" +
                "assetPairId='" + assetPairId + '\'' +
                ", clientId='" + clientId + '\'' +
                ", volume=" + volume +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", registered=" + registered +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
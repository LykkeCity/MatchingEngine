package com.lykke.matching.engine.daos.azure;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class AzureOrderTradesLink extends TableServiceEntity {
    //partition key: order id
    //row key: trade id

    public AzureOrderTradesLink() {
    }

    public AzureOrderTradesLink(String orderId, String tradeId) {
        super(orderId, tradeId);
    }

    public String getOrderId() {
        return partitionKey;
    }

    public String getTradeId() {
        return rowKey;
    }
}
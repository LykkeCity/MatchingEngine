package com.lykke.matching.engine.daos;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class OrderTradesLink extends TableServiceEntity {
    //partition key: order id
    //row key: trade id

    public OrderTradesLink() {
    }

    public OrderTradesLink(String orderId, String tradeId) {
        super(orderId, tradeId);
    }

    public String getOrderId() {
        return partitionKey;
    }

    public String getTradeId() {
        return rowKey;
    }
}
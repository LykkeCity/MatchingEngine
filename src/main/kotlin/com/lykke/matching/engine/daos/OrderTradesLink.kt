package com.lykke.matching.engine.daos

import com.microsoft.azure.storage.table.TableServiceEntity

class OrderTradesLink(orderId: String, tradeId: String): TableServiceEntity(orderId, tradeId) {
    //partition key: order id
    //row key: trade id
}
package com.lykke.matching.engine.order.process.common

import com.lykke.matching.engine.daos.LimitOrder
import com.lykke.matching.engine.daos.fee.v2.NewLimitOrderFeeInstruction
import com.lykke.matching.engine.daos.order.LimitOrderType
import com.lykke.matching.engine.daos.v2.LimitOrderFeeInstruction
import com.lykke.matching.engine.order.OrderStatus
import java.util.Date

class OrderUtils {
    companion object {
        fun createChildLimitOrder(order: LimitOrder,
                                  newOrderId: String,
                                  newOrderExternalId: String,
                                  date: Date): LimitOrder {
            return LimitOrder(newOrderId,
                    newOrderExternalId,
                    order.assetPairId,
                    order.clientId,
                    order.volume,
                    order.price,
                    OrderStatus.InOrderBook.name,
                    date,
                    date,
                    date,
                    order.remainingVolume,
                    null,
                    null,
                    order.fee as? LimitOrderFeeInstruction,
                    order.fees?.map { it as NewLimitOrderFeeInstruction },
                    LimitOrderType.LIMIT,
                    null,
                    null,
                    null,
                    null,
                    null,
                    order.timeInForce,
                    order.expiryTime,
                    order.externalId,
                    null)
        }
    }
}
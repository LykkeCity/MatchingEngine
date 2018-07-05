package com.lykke.matching.engine.database.common.entity

import com.lykke.matching.engine.deduplication.ProcessedMessage
import org.springframework.util.CollectionUtils

class PersistenceData(val balancesData: BalancesData?,
                      val processedMessage: ProcessedMessage? = null,
                      val orderBooksData: OrderBooksPersistenceData?,
                      val stopOrderBooksData: OrderBooksPersistenceData?,
                      val messageSequenceNumber: Long?) {

    constructor(processedMessage: ProcessedMessage?, messageSequenceNumber: Long?) : this(null, processedMessage, null, null, messageSequenceNumber)

    fun details(): String {
        val result = StringBuilder()
        append(result, "m: ", processedMessage?.messageId)
        append(result, "w: ", balancesData?.wallets?.size)
        append(result, "b: ", balancesData?.balances?.size)
        append(result, "o: ", orderBooksData?.orderBooks?.size)
        append(result, "so: ", stopOrderBooksData?.orderBooks?.size)
        append(result, "sn: ", messageSequenceNumber)
        return result.toString()
    }

    fun isEmpty(): Boolean {
        return CollectionUtils.isEmpty(balancesData?.balances) &&
                CollectionUtils.isEmpty(balancesData?.wallets) &&
                processedMessage == null &&
                messageSequenceNumber == null &&
                CollectionUtils.isEmpty(orderBooksData?.orderBooks) &&
                CollectionUtils.isEmpty(orderBooksData?.ordersToSave) &&
                CollectionUtils.isEmpty(orderBooksData?.ordersToRemove) &&
                CollectionUtils.isEmpty(stopOrderBooksData?.orderBooks) &&
                CollectionUtils.isEmpty(stopOrderBooksData?.ordersToSave) &&
                CollectionUtils.isEmpty(stopOrderBooksData?.ordersToRemove)
    }

    private fun append(builder: StringBuilder, prefix: String, obj: Any?) {
        obj?.let {
            if (builder.isNotEmpty()) {
                builder.append(", ")
            }
            builder.append(prefix).append(obj)
        }
    }
}